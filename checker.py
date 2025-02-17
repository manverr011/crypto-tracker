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

# ✅ Step 1: Authenticate with Google Sheets
credentials_json = os.getenv("GOOGLE_CREDENTIALS")
if not credentials_json:
    raise ValueError("Missing Google credentials in environment variables")

creds_dict = json.loads(credentials_json)
creds_file = StringIO(json.dumps(creds_dict))

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
SHEET_NAME = os.getenv("SHEET_NAME", "Crypto_Tracker")
if not SHEET_NAME:
    raise ValueError("Missing SHEET_NAME environment variable")

sheet = client.open(SHEET_NAME).sheet1

# ✅ Step 2: Fetch All USDT Trading Pairs
BINANCE_API_KEY = os.getenv("MiHw4ZyTFiZVDwGxdORAW0PbXqzchwGLmWoE25tt0XDoGnv436T3N0nA3tQvSVYg", "")  # Binance API Key
BINANCE_API_URL = "https://api.binance.com/api/v3/exchangeInfo"

async def get_usdt_pairs():
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}
    retries = 5
    delay = 1  # Initial delay

    async with aiohttp.ClientSession() as session:
        for attempt in range(retries):
            try:
                async with session.get(BINANCE_API_URL, headers=headers) as response:
                    if response.status == 429:
                        print(f"⚠️ Rate limit hit! Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        delay *= 2  # Exponential backoff
                        continue
                    elif response.status != 200:
                        print(f"❌ Error {response.status}: {await response.text()}")
                        return []

                    data = await response.json()
                    return [s["symbol"] for s in data.get("symbols", []) if s["symbol"].endswith("USDT")]
            except Exception as e:
                print(f"⚠️ Exception: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay *= 2  # Increase delay
    
    print("❌ Failed to fetch USDT pairs.")
    return []

# ✅ Step 3: Fetch Live Prices in Batches
async def fetch_prices(symbols):
    url = "https://api.binance.com/api/v3/ticker/price"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                return {item["symbol"]: float(item["price"]) for item in data if item["symbol"] in symbols}
    except Exception as e:
        print(f"⚠️ Error fetching prices: {e}")
        return {}

# ✅ Step 4: Fetch Previous Day's Closing Prices
async def fetch_historical_data(symbols):
    url = "https://api.binance.com/api/v3/klines"
    end_time = int(datetime.utcnow().timestamp() * 1000)
    start_time = int((datetime.utcnow() - timedelta(days=1)).timestamp() * 1000)
    closing_prices = {}

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            try:
                async with session.get(url, params={
                    "symbol": symbol,
                    "interval": "1d",
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": 1
                }) as response:
                    data = await response.json()
                    closing_prices[symbol] = float(data[0][4]) if data else "N/A"
            except Exception as e:
                print(f"⚠️ Error fetching historical data for {symbol}: {e}")
                closing_prices[symbol] = "N/A"
    return closing_prices

# ✅ Step 5: Update Google Sheets
async def update_google_sheet():
    usdt_pairs = await get_usdt_pairs()
    if not usdt_pairs:
        print("❌ No USDT pairs found, skipping update.")
        return
    
    live_prices, closing_prices = await asyncio.gather(
        fetch_prices(usdt_pairs),
        fetch_historical_data(usdt_pairs)
    )

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    update_data = [["Symbol", "Price", "Last Close", "Updated At"]]
    update_data += [[s, live_prices.get(s, "N/A"), closing_prices.get(s, "N/A"), timestamp] for s in usdt_pairs]

    try:
        sheet.update("A1", update_data)
        print(f"[{timestamp}] ✅ Google Sheet updated successfully!")
    except Exception as e:
        print(f"⚠️ Error updating Google Sheets: {e}")

# ✅ Step 6: Start Web Server for Render
async def handle(request):
    return web.Response(text="Server running - Binance Tracker is active!")

app = web.Application()
app.router.add_get("/", handle)

async def start_server():
    port = int(os.getenv("PORT", "8080"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"✅ Server running on port {port}!")

# ✅ Step 7: Run Everything
async def main():
    await asyncio.gather(
        start_server(),
        update_google_sheet()
    )

if __name__ == "__main__":
    asyncio.run(main())
