import os
import json
from io import StringIO
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# Read credentials from environment variable
credentials_json = os.getenv("GOOGLE_CREDENTIALS")
creds_dict = json.loads(credentials_json)
creds_file = StringIO(json.dumps(creds_dict))

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

SHEET_NAME = "USDT_Pairs_Tracker"
sheet = client.open(SHEET_NAME).sheet1

# ---- Step 2: Fetch all USDT pairs ----
async def get_usdt_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return [s["symbol"] for s in data["symbols"] if s["symbol"].endswith("USDT")]

# ---- Step 3: Fetch real-time prices ----
async def get_prices(session, symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    async with session.get(url) as response:
        data = await response.json()
        return symbol, float(data["price"])

# ---- Step 4: Fetch previous day's closing price ----
async def get_last_closing_price(session, symbol):
    url = "https://api.binance.com/api/v3/klines"
    end_time = int(datetime.utcnow().timestamp() * 1000)
    start_time = int((datetime.utcnow() - timedelta(days=1)).timestamp() * 1000)
    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1
    }
    async with session.get(url, params=params) as response:
        data = await response.json()
        if data:
            return symbol, float(data[0][4])  # Closing price
        return symbol, None

# ---- Step 5: Update Google Sheets ----
async def update_google_sheet():
    usdt_pairs = await get_usdt_pairs()
    async with aiohttp.ClientSession() as session:
        price_tasks = [get_prices(session, pair) for pair in usdt_pairs]
        close_tasks = [get_last_closing_price(session, pair) for pair in usdt_pairs]
        
        prices = await asyncio.gather(*price_tasks)
        closing_prices = await asyncio.gather(*close_tasks)

    price_dict = dict(prices)
    closing_dict = dict(closing_prices)
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    update_data = [["Symbol", "Current Price", "Last Close Price", "Updated At"]] + \
                  [[s, price_dict[s], closing_dict.get(s, "N/A"), timestamp] for s in usdt_pairs]
    
    sheet.update("A1", update_data)
    print(f"[{timestamp}] Google Sheet updated successfully!")

# ---- Step 6: Run the script every second ----
async def main():
    while True:
        await update_google_sheet()
        await asyncio.sleep(1)  # Update every second

asyncio.run(main())
