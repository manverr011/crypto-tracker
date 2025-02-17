import asyncio
import aiohttp
import gspread
import os
import json
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import random
from io import StringIO
from aiohttp import web

# ---- Step 1: Authenticate with Google Sheets ----
credentials_json = os.getenv("GOOGLE_CREDENTIALS")
if not credentials_json:
    raise ValueError("Missing Google credentials in environment variables")

creds_dict = json.loads(credentials_json)
creds_file = StringIO(json.dumps(creds_dict))

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
SHEET_NAME = os.getenv("SHEET_NAME", "Crypto_Tracker")  # Default value added
if not SHEET_NAME:
    raise ValueError("Missing SHEET_NAME environment variable")

sheet = client.open(SHEET_NAME).sheet1  # First sheet

# ---- Step 2: Fetch All USDT Trading Pairs ----
async def get_usdt_pairs():
    url = "url = "https://api.binance.us/api/v3/exchangeInfo"
    max_retries = 5

    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status} - Binance API issue")

                    data = await response.json()

                    # Debug: Print response to check if it contains 'symbols'
                    if "symbols" not in data:
                        print(f"⚠️ Unexpected Binance response: {data}")
                        raise KeyError("Missing 'symbols' in Binance response")

                    return [s["symbol"] for s in data["symbols"] if s["symbol"].endswith("USDT")]

        except (aiohttp.ClientError, KeyError, Exception) as e:
            wait_time = 2 ** attempt + random.uniform(0, 1)  # Exponential backoff
            print(f"⚠️ Error fetching USDT pairs (attempt {attempt + 1}): {e}. Retrying in {wait_time:.2f}s...")
            await asyncio.sleep(wait_time)

    print("❌ Failed to fetch USDT pairs after multiple attempts.")
    return []

# ---- Step 3: Fetch Live Prices in Batches ----
async def fetch_prices(symbols):
    url = "https://api.binance.com/api/v3/ticker/price"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url) as response:
                data = await response.json()
                return {item["symbol"]: float(item["price"]) for item in data if item["symbol"] in symbols}
    except Exception as e:
        print(f"⚠️ Error fetching prices: {e}")
        return {}

# ---- Step 4: Fetch Previous Day's Closing Prices ----
async def fetch_historical_data(symbols):
    url = "https://api.binance.com/api/v3/klines"
    end_time = int(datetime.utcnow().timestamp() * 1000)
    start_time = int((datetime.utcnow() - timedelta(days=1)).timestamp() * 1000)
    
    closing_prices = {}
    batch_size = 10  # ✅ Fetch data in smaller batches to avoid overload

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            tasks = [
                session.get(url, params={
                    "symbol": symbol,
                    "interval": "1d",
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": 1
                }) for symbol in batch_symbols
            ]
            
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for symbol, response in zip(batch_symbols, responses):
                if isinstance(response, Exception):
                    print(f"⚠️ Error fetching historical data for {symbol}: {response}")
                    closing_prices[symbol] = "N/A"
                    continue
                
                try:
                    data = await response.json()
                    closing_prices[symbol] = float(data[0][4]) if data else "N/A"
                except Exception as e:
                    print(f"⚠️ Error parsing data for {symbol}: {e}")
                    closing_prices[symbol] = "N/A"

    return closing_prices

# ---- Step 5: Update Google Sheets ----
async def update_google_sheet():
    usdt_pairs = await get_usdt_pairs()
    if not usdt_pairs:
        print("❌ No USDT pairs found, skipping update.")
        return
    
    # Fetch live and historical prices
    live_prices, closing_prices = await asyncio.gather(
        fetch_prices(usdt_pairs),
        fetch_historical_data(usdt_pairs)
    )

    # Prepare data for Google Sheets
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    update_data = [["Symbol", "Price", "Last Close", "Updated At"]]
    update_data += [[s, live_prices.get(s, "N/A"), closing_prices.get(s, "N/A"), timestamp] for s in usdt_pairs]

    try:
        sheet.update("A1", update_data)
        print(f"[{timestamp}] ✅ Google Sheet updated successfully!")
    except Exception as e:
        print(f"⚠️ Error updating Google Sheets: {e}")

# ---- Step 6: Start Dummy Web Server for Render ----
async def handle(request):
    return web.Response(text="Server running - Binance Tracker is active!")

app = web.Application()
app.router.add_get("/", handle)

async def start_server():
    port = int(os.getenv("PORT", "8080"))  # ✅ Default to 8080 if no PORT is set
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    print(f"✅ Starting server on port {port}...")
    
    try:
        await site.start()
        print(f"✅ Server running on port {port}!")
    except OSError as e:
        print(f"❌ Port binding error: {e}. Retrying with port 8080...")
        await site.stop()
        site = web.TCPSite(runner, "0.0.0.0", 8080)
        await site.start()
        print("✅ Server started on fallback port 8080.")

# ---- Step 7: Run Everything ----
async def main():
    await asyncio.gather(
        start_server(),  # Keeps Render running
        update_google_sheet()  # Crypto_Tracker updates Google Sheets
    )

if __name__ == "__main__":
    asyncio.run(main())
