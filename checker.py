import os
import json
import asyncio
import aiohttp
import gspread
from io import StringIO
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import random

# Read credentials from environment variable
credentials_json = os.getenv("GOOGLE_CREDENTIALS")
if not credentials_json:
    raise ValueError("GOOGLE_CREDENTIALS environment variable not set.")

creds_dict = json.loads(credentials_json)
creds_file = StringIO(json.dumps(creds_dict))

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

SHEET_NAME = os.getenv("SHEET_NAME", "Crypto_Tracker")  # Ensure this matches your Google Sheet name
try:
    sheet = client.open(SHEET_NAME).sheet1  # First sheet
except gspread.exceptions.SpreadsheetNotFound:
    raise ValueError(f"Spreadsheet '{SHEET_NAME}' not found. Check if the name is correct.")

# ---- Step 2: Fetch All USDT Trading Pairs ----
async def get_usdt_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    max_retries = 5
    
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(url) as response:
                    data = await response.json()
                    
                    if "symbols" not in data:
                        raise KeyError("Missing 'symbols' in Binance response")

                    return [s["symbol"] for s in data["symbols"] if s["symbol"].endswith("USDT")]

        except (aiohttp.ClientError, KeyError) as e:
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

    # ✅ Google Sheets Rate Limit Fix - Splitting updates into chunks
    chunk_size = 50
    for i in range(0, len(update_data), chunk_size):
        try:
            sheet.batch_update([{"range": f"A{i+1}", "values": update_data[i:i+chunk_size]}])
            print(f"[{timestamp}] ✅ Google Sheet updated successfully! ({i+1}-{i+chunk_size})")
        except Exception as e:
            print(f"⚠️ Error updating Google Sheets: {e}")
            await asyncio.sleep(5)  # Wait before retrying to avoid rate limits

# ---- Step
