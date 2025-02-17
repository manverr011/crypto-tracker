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

# ---- Google Sheets Authentication ----
credentials_json = os.getenv("GOOGLE_CREDENTIALS")
if not credentials_json:
    raise ValueError("Missing Google credentials in environment variables")

creds_dict = json.loads(credentials_json)
creds_file = StringIO(json.dumps(creds_dict))

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
SHEET_NAME = os.getenv("SHEET_NAME", "Crypto_Tracker")
if not SHEET_NAME:
    raise ValueError("Missing SHEET_NAME environment variable")

sheet = client.open(SHEET_NAME).sheet1  # First sheet

# ---- Binance API Configuration ----
BINANCE_API_KEY = os.getenv("api")  # Load from environment
HEADERS = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}

# ---- Fetch All USDT Trading Pairs ----
async def get_usdt_pairs():
    url = "https://api.binance.us/api/v3/exchangeInfo"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5),
            connector=aiohttp.TCPConnector(ssl=False)
        ) as session:
            async with session.get(url, headers=HEADERS) as response:
                data = await response.json()
                return [s["symbol"] for s in data.get("symbols", []) if s["symbol"].endswith("USDT")]
    except Exception as e:
        print(f"⚠️ Error fetching USDT pairs: {e}")
        return []

# ---- Fetch Live Prices ----
async def fetch_prices(symbols):
    url = "https://api.binance.us/api/v3/ticker/price"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5),
            connector=aiohttp.TCPConnector(ssl=False)
        ) as session:
            async with session.get(url, headers=HEADERS) as response:
                data = await response.json()
                return {item["symbol"]: float(item["price"]) for item in data if item["symbol"] in symbols}
    except Exception as e:
        print(f"⚠️ Error fetching prices: {e}")
        return {}

# ---- Fetch Previous Day's Closing Prices ----
async def fetch_historical_data(symbols):
    url = "https://api.binance.us/api/v3/klines"
    end_time = int(datetime.utcnow().timestamp() * 1000)
    start_time = int((datetime.utcnow() - timedelta(days=1)).timestamp() * 1000)
    closing_prices = {}

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=5),
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        tasks = [
            session.get(url, params={
                "symbol": symbol,
                "interval": "1d",
                "startTime": start_time,
                "endTime": end_time,
                "limit": 1
            }, headers=HEADERS) for symbol in symbols
        ]

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for symbol, response in zip(symbols, responses):
            if isinstance(response, Exception):
                closing_prices[symbol] = "N/A"
                continue

            try:
                data = await response.json()
                closing_prices[symbol] = float(data[0][4]) if data else "N/A"
            except:
                closing_prices[symbol] = "N/A"

    return closing_prices

# ---- Update Google Sheets Every Second ----
async def update_google_sheet():
    while True:
        try:
            usdt_pairs = await get_usdt_pairs()
            if not usdt_pairs:
                print("⚠️ No USDT pairs found. Retrying in 5 seconds...")
                await asyncio.sleep(5)
                continue
            
            live_prices, closing_prices = await asyncio.gather(
                fetch_prices(usdt_pairs),
                fetch_historical_data(usdt_pairs)
            )

            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            update_data = [["Symbol", "Price", "Last Close", "Updated At"]]
            update_data += [[s, live_prices.get(s, "N/A"), closing_prices.get(s, "N/A"), timestamp] for s in usdt_pairs]

            sheet.update(range_name="A1", values=update_data)
            print(f"[{timestamp}] ✅ Google Sheet updated!")
        except Exception as e:
            print(f"⚠️ Error updating Google Sheets: {e}. Retrying in 5 seconds...")
        
        await asyncio.sleep(5)  # Update every 5 seconds

# ---- Start Web Server ----
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

# ---- Run Everything ----
async def main():
    await asyncio.gather(
        start_server(),
        update_google_sheet()
    )

if __name__ == "__main__":
    asyncio.run(main())
