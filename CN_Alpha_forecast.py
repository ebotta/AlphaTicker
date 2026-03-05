import csv
import os
import requests
import statistics
from fileinput import close

def read_api_key_from_file():
    try:
        with open('key.txt', 'r') as file:
            api_key = file.read().strip()
            return api_key
    except FileNotFoundError:
        print("API key file not found. Please create a file named 'api_key.txt' and place your API key inside it.")
        return None

api_key = read_api_key_from_file()

def _get_field(item, keys):
    for k in keys:
        if isinstance(item, dict) and k in item:
            return item[k]
    return None

# pulling GDX for comparison
url = f"https://eodhd.com/api/eod/GDX.US?api_token={api_key}&fmt=json&period=d&from=2024-12-31&to=2025-12-31"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

resp = requests.get(url, headers=headers, timeout=15)

j = resp.json()

# Try common locations for time series
if isinstance(j, dict):
    items = j.get('data') or j.get('results') or j.get('series') or []
elif isinstance(j, list):
    items = j
else:
    items = []

GDX = []
for item in items:
    d = _get_field(item, ('d', 'date'))
    c = _get_field(item, ('c', 'close', 'price'))
    v = _get_field(item, ('v', 'volume'))
    try:
        c = float(c) if c is not None else None
        v = float(v) if v is not None else None
    except Exception:
        c = None
        v = None
    if d is None or c is None or v is None:
        continue
    GDX.append({'d': d, 'c': c, 'v': v})
# length of GDX data pulled
print(f"Length of GDX data pulled: {len(GDX)}")

CN_tickers = []

# Read tickers from portfolio text file
try:
    filename = 'portfolio.txt'

    with open(filename, 'r') as file:
        for line in file:
            # Process each line here
            CN_tickers.append(line.strip())
            # The 'line' variable will have a trailing newline character ('\n')
except FileNotFoundError:
    print("Error: 'portfolio.txt' not found.")
except Exception as e:
    print(f"An error occurred while reading text file: {e}")

print(f"Total tickers read: {len(CN_tickers)}")
# print all the tickers read from the file
print(f"Sample tickers: {CN_tickers}")

for ticker in CN_tickers:
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d&from=2024-12-31&to=2025-12-31"

    try:
        resp = requests.get(url, headers=headers, timeout=15)
    except Exception as e:
        print(f"Request failed for {ticker}: {e}")
        continue

    if not resp.ok:
        print(f"Non-OK response for {ticker}: {resp.status_code}")
        continue

    try:
        j = resp.json()
    except Exception as e:
        print(f"Invalid JSON for {ticker}: {e}")
        continue

    # Try common locations for time series
    if isinstance(j, dict):
        items = j.get('data') or j.get('results') or j.get('series') or []
    elif isinstance(j, list):
        items = j
    else:
        items = []

    extracted = []
    for item in items:
        d = _get_field(item, ('d', 'date'))
        c = _get_field(item, ('c', 'close', 'price'))
        v = _get_field(item, ('v', 'volume'))
        try:
            c = float(c) if c is not None else None
            v = float(v) if v is not None else None
        except Exception:
            c = None
            v = None
        if d is None or c is None or v is None:
            continue
        extracted.append({'d': d, 'c': c, 'v': v})

    if not extracted:
        print("No usable time-series data found for this ticker.")
        print("-" * 20)
        continue

    dates = [item['d'] for item in extracted]
    closes = [item['c'] for item in extracted]
    volumes = [item['v'] for item in extracted]

    ticker_returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
    GDX_returns = [(GDX[i]['c'] - GDX[i-1]['c']) / GDX[i-1]['c'] for i in range(1, len(GDX))]

    difference = [ticker_returns[i] - GDX_returns[i] for i in range(min(len(ticker_returns), len(GDX_returns)))]

    z_score_price = [(difference[i] - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) for i in range(len(difference))]
    z_score_volume = [(volumes[i] - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) for i in range(len(volumes))]

    # Find the closes closest to the target dates
    day0_date = dates[0]  # Assuming the first date in the data is the oldest date
    day30_date = dates[21] if len(dates) > 21 else dates[-1]
    day90_date = dates[63] if len(dates) > 63 else dates[-1]
    day180_date = dates[126] if len(dates) > 126 else dates[-1]
    day360_date = dates[-1]  # Assuming the last date in the data is the most recent date            

    close0 = closes[0]  # Assuming the first close in the data is the oldest close
    volume0 = volumes[0]  # Assuming the first volume in the data is the oldest volume
    
    GDX_close0 = GDX[0]['c'] if len(GDX) > 0 else None
    GDX_volume0 = GDX[0]['v'] if len(GDX) > 0 else None

    if volume0 == 0.0:
        print(f"Warning: volume0 is zero for {ticker}, skipping net30_volume calculation to avoid division by zero.")
        continue
    if GDX_volume0 == 0.0:
        print(f"Warning: GDX_volume0 is zero, skipping net30_volume calculation to avoid division by zero.")
        continue
    if close0 == 0.0:
        print(f"Warning: close0 is zero for {ticker}, skipping net30_close calculation to avoid division by zero.")
        continue
    if GDX_close0 == 0.0:
        print(f"Warning: GDX_close0 is zero, skipping net30_close calculation to avoid division by zero.")
        continue

    close30 = closes[21] if len(closes) > 21 else closes[-1]
    close30_return = (close30 - close0) / close0 if close0 else None
    GDX_close30 = GDX[21]['c'] if len(GDX) > 21 else GDX[-1]['c'] if len(GDX) > 0 else None
    GDX_close30_return = (GDX_close30 - GDX_close0) / GDX_close0 if GDX_close0 else None
    net30_close = close30_return - GDX_close30_return if close30_return is not None and GDX_close30_return is not None else None
    zscore30_price = (net30_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net30_close is not None else None

    volume30 = volumes[21] if len(volumes) > 21 else volumes[-1]
    volume30_return = (volume30 - volume0) / volume0 if volume0 else None
    GDX_volume30 = GDX[21]['v'] if len(GDX) > 21 else GDX[-1]['v'] if len(GDX) > 0 else None
    GDX_volume30_return = (GDX_volume30 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
    net30_volume = volume30_return - GDX_volume30_return if volume30_return is not None and GDX_volume30_return is not None else None
    zscore30_volume = (net30_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net30_volume is not None else None

    write_mode = 'a' if os.path.exists('forecast.csv') else 'w'
    with open('forecast.csv', mode=write_mode, newline='', encoding='utf-8') as forecast_file:
        forecast_writer = csv.writer(forecast_file)
        if write_mode == 'w':
            forecast_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
        forecast_writer.writerow([ticker, '30', day30_date, f"{zscore30_price:.2f}", f"{zscore30_volume:.2f}"])     

    
    close90 = closes[63] if len(closes) > 63 else closes[-1]
    close90_return = (close90 - close0) / close0 if close0 else None
    GDX_close90 = GDX[63]['c'] if len(GDX) > 63 else GDX[-1]['c'] if len(GDX) > 0 else None
    GDX_close90_return = (GDX_close90 - GDX_close0) / GDX_close0 if GDX_close0 else None
    net90_close = close90_return - GDX_close90_return if close90_return is not None and GDX_close90_return is not None else None
    zscore90_price = (net90_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net90_close is not None else None

    volume90 = volumes[63] if len(volumes) > 63 else volumes[-1]
    volume90_return = (volume90 - volume0) / volume0 if volume0 else None
    GDX_volume90 = GDX[63]['v'] if len(GDX) > 63 else GDX[-1]['v'] if len(GDX) > 0 else None
    GDX_volume90_return = (GDX_volume90 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
    net90_volume = volume90_return - GDX_volume90_return if volume90_return is not None and GDX_volume90_return is not None else None
    zscore90_volume = (net90_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net90_volume is not None else None

    write_mode = 'a' if os.path.exists('forecast.csv') else 'w'
    with open('forecast.csv', mode=write_mode, newline='', encoding='utf-8') as forecast_file:
        forecast_writer = csv.writer(forecast_file)
        if write_mode == 'w':
            forecast_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
        forecast_writer.writerow([ticker, '90', day90_date, f"{zscore90_price:.2f}", f"{zscore90_volume:.2f}"])

    close180 = closes[126] if len(closes) > 126 else closes[-1]
    close180_return = (close180 - close0) / close0 if close0 else None
    GDX_close180 = GDX[126]['c'] if len(GDX) > 126 else GDX[-1]['c'] if len(GDX) > 0 else None
    GDX_close180_return = (GDX_close180 - GDX_close0) / GDX_close0 if GDX_close0 else None
    net180_close = close180_return - GDX_close180_return if close180_return is not None and GDX_close180_return is not None else None
    zscore180_price = (net180_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net180_close is not None else None

    volume180 = volumes[126] if len(volumes) > 126 else volumes[-1]
    volume180_return = (volume180 - volume0) / volume0 if volume0 else None
    GDX_volume180 = GDX[126]['v'] if len(GDX) > 126 else GDX[-1]['v'] if len(GDX) > 0 else None
    GDX_volume180_return = (GDX_volume180 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
    net180_volume = volume180_return - GDX_volume180_return if volume180_return is not None and GDX_volume180_return is not None else None
    zscore180_volume = (net180_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net180_volume is not None else None

    write_mode = 'a' if os.path.exists('forecast.csv') else 'w'
    with open('forecast.csv', mode=write_mode, newline='', encoding='utf-8') as forecast_file:
        forecast_writer = csv.writer(forecast_file)
        if write_mode == 'w':
            forecast_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
        forecast_writer.writerow([ticker, '180', day180_date, f"{zscore180_price:.2f}", f"{zscore180_volume:.2f}"])
    
    close360 = closes[-1]  # Assuming the last close in the data is the most recent close
    close360_return = (close360 - close0) / close0 if close0 else None
    GDX_close360 = GDX[-1]['c'] if len(GDX) > 0 else None
    GDX_close360_return = (GDX_close360 - GDX_close0) / GDX_close0 if GDX_close0 else None
    net360_close = close360_return - GDX_close360_return if close360_return is not None and GDX_close360_return is not None else None
    zscore360_price = (net360_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net360_close is not None else None

    volume360 = volumes[-1]  # Assuming the last volume in the data is the most recent volume
    volume360_return = (volume360 - volume0) / volume0 if volume0 else None
    GDX_volume360 = GDX[-1]['v'] if len(GDX) > 0 else None
    GDX_volume360_return = (GDX_volume360 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
    net360_volume = volume360_return - GDX_volume360_return if volume360_return is not None and GDX_volume360_return is not None else None
    zscore360_volume = (net360_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net360_volume is not None else None    

    write_mode = 'a' if os.path.exists('forecast.csv') else 'w'
    with open('forecast.csv', mode=write_mode, newline='', encoding='utf-8') as forecast_file:
        forecast_writer = csv.writer(forecast_file)
        if write_mode == 'w':
            forecast_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
        forecast_writer.writerow([ticker, '360', day360_date, f"{zscore360_price:.2f}", f"{zscore360_volume:.2f}"])
