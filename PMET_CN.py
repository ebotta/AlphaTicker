import csv
import os
import requests
import statistics

def read_api_key_from_file():
    try:
        with open('key.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        print("Error: 'key.txt' not found.")
        return None

api_key = read_api_key_from_file()  # Implement this function to read your API key from a file or environment variable

def _get_field(item, keys):
    for k in keys:
        if isinstance(item, dict) and k in item:
            return item[k]
    return None

# pulling GDX for comparison
url = f"https://eodhd.com/api/eod/GDX.US?api_token={api_key}&fmt=json&period=d&from=2023-12-31&to=2024-12-31"

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

CN_tickers = []

# Read tickers from CSV (safe indexing)
try:
    with open('tickers.csv', mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            # print(f"Original line tokens: {row}")
            if not row:
                print("Empty row, skipping")
                continue
            # print(f"Raw row: {row}")
            first = row[0].strip() if len(row) >= 1 else ''
            second = row[1].strip().upper() if len(row) >= 2 else ''
            fourth = row[3].strip() if len(row) >= 4 else ''
            if second == 'CN':
                CN_tickers.append(first + "," + fourth)

except FileNotFoundError:
    print("Error: 'tickers.csv' not found.")
except Exception as e:
    print(f"An error occurred while reading CSV: {e}")

# If no CN tickers found, keep a default example ticker
if not CN_tickers:
    CN_tickers = ['AEM']

headers = {}

for ticker in CN_tickers:
    if ticker.split(',')[1] == "Toronto":
        CN_SE = "TO"
    elif ticker.split(',')[1] == "Venture":
        CN_SE = "V"
    elif ticker.split(',')[1] == "NEO-L":   
        CN_SE = "NEO"
    elif ticker.split(',')[1] == "Canadian":
        continue


    url = f"https://eodhd.com/api/fundamentals/{ticker.split(',')[0]}.{CN_SE}?filter=Highlights::MarketCapitalizationMln&api_token={api_key}&fmt=json"

    try:
        resp = requests.get(url, headers=headers, timeout=15)
    except Exception as e:
        continue
    if not resp.ok:
        with open('fails.csv', 'a', newline='') as out_file:
            out_ticker = csv.writer(out_file)
            out_ticker.writerow([ticker])
        continue
        
    try:        j = resp.json()
    except Exception as e:
        continue

    if str(j).lower() == "na":
        continue

    if int(j) > 50:
        url = f"https://eodhd.com/api/eod/{ticker.split(',')[0]}.{CN_SE}?api_token={api_key}&fmt=json&period=d&from=2023-12-31&to=2024-12-31"
        
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

        threshold = 5
        vol_threshold = -3

        avg_volume = sum(volumes) / len(volumes) if volumes else 0

        if avg_volume > 100000:

            # Find the closes closest to the target dates
            day0_date = dates[0]  # Assuming the first date in the data is the oldest date
            day30_date = dates[21] if len(dates) > 21 else dates[-1]
            day60_date = dates[42] if len(dates) > 42 else dates[-1]
            day90_date = dates[63] if len(dates) > 63 else dates[-1]
            day120_date = dates[84] if len(dates) > 84 else dates[-1]
            day150_date = dates[101] if len(dates) > 101 else dates[-1]
            day180_date = dates[122] if len(dates) > 122 else dates[-1]
            day210_date = dates[143] if len(dates) > 143 else dates[-1]
            day240_date = dates[164] if len(dates) > 164 else dates[-1]
            day270_date = dates[185] if len(dates) > 185 else dates[-1]
            day300_date = dates[206] if len(dates) > 206 else dates[-1]
            day330_date = dates[227] if len(dates) > 227 else dates[-1]
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

            if zscore30_price is not None and zscore30_price > threshold and zscore30_volume is not None and zscore30_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '30', day30_date, f"{zscore30_price:.2f}", f"{zscore30_volume:.2f}"])
            
            close60 = closes[42] if len(closes) > 42 else closes[-1]
            close60_return = (close60 - close0) / close0 if close0 else None
            GDX_close60 = GDX[42]['c'] if len(GDX) > 42 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close60_return = (GDX_close60 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net60_close = close60_return - GDX_close60_return if close60_return is not None and GDX_close60_return is not None else None
            zscore60_price = (net60_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net60_close is not None else None
                        
            volume60 = volumes[42] if len(volumes) > 42 else volumes[-1]
            volume60_return = (volume60 - volume0) / volume0 if volume0 else None
            GDX_volume60 = GDX[42]['v'] if len(GDX) > 42 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume60_return = (GDX_volume60 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net60_volume = volume60_return - GDX_volume60_return if volume60_return is not None and GDX_volume60_return is not None else None
            zscore60_volume = (net60_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net60_volume is not None else None
            
            if zscore60_price is not None and zscore60_price > threshold and zscore60_volume is not None and zscore60_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '60', day60_date, f"{zscore60_price:.2f}", f"{zscore60_volume:.2f}"])

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
            net90_volume = ((volume90 - volume0) / volume0 if volume0 else None) - ((GDX_volume90 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore90_volume = (net90_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net90_volume is not None else None
            
            if zscore90_price is not None and zscore90_price > threshold and zscore90_volume is not None and zscore90_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '90', day90_date, f"{zscore90_price:.2f}", f"{zscore90_volume:.2f}"])
            
            close120 = closes[84] if len(closes) > 84 else closes[-1]
            close120_return = (close120 - close0) / close0 if close0 else None
            GDX_close120 = GDX[84]['c'] if len(GDX) > 84 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close120_return = (GDX_close120 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net120_close = close120_return - GDX_close120_return if close120_return is not None and GDX_close120_return is not None else None
            zscore120_price = (net120_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net120_close is not None else None
            
            volume120 = volumes[84] if len(volumes) > 84 else volumes[-1]
            volume120_return = (volume120 - volume0) / volume0 if volume0 else None
            GDX_volume120 = GDX[84]['v'] if len(GDX) > 84 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume120_return = (GDX_volume120 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net120_volume = ((volume120 - volume0) / volume0 if volume0 else None) - ((GDX_volume120 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore120_volume = (net120_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net120_volume is not None else None
            
            if zscore120_price is not None and zscore120_price > threshold and zscore120_volume is not None and zscore120_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '120', day120_date, f"{zscore120_price:.2f}", f"{zscore120_volume:.2f}"])

            close150 = closes[101] if len(closes) > 101 else closes[-1]
            close150_return = (close150 - close0) / close0 if close0 else None
            GDX_close150 = GDX[101]['c'] if len(GDX) > 101 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close150_return = (GDX_close150 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net150_close = close150_return - GDX_close150_return if close150_return is not None and GDX_close150_return is not None else None
            zscore150_price = (net150_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net150_close is not None else None
           
            volume150 = volumes[101] if len(volumes) > 101 else volumes[-1]
            GDX_volume150 = GDX[101]['v'] if len(GDX) > 101 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume150_return = (GDX_volume150 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net150_close = close150_return - GDX_close150_return if close150_return is not None and GDX_close150_return is not None else None
            net150_volume = ((volume150 - volume0) / volume0 if volume0 else None) - ((GDX_volume150 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore150_volume = (net150_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net150_volume is not None else None
           
            if zscore150_price is not None and zscore150_price > threshold and zscore150_volume is not None and zscore150_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '150', day150_date, f"{zscore150_price:.2f}", f"{zscore150_volume:.2f}"])

            close180 = closes[122] if len(closes) > 122 else closes[-1]
            close180_return = (close180 - close0) / close0 if close0 else None
            GDX_close180 = GDX[122]['c'] if len(GDX) > 122 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close180_return = (GDX_close180 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net180_close = close180_return - GDX_close180_return if close180_return is not None and GDX_close180_return is not None else None
            zscore180_price = (net180_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net180_close is not None else None
           
            volume180 = volumes[122] if len(volumes) > 122 else volumes[-1]
            close180_return = (close180 - close0) / close0 if close0 else None
            GDX_volume180 = GDX[122]['v'] if len(GDX) > 122 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume180_return = (GDX_volume180 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net180_volume = ((volume180 - volume0) / volume0 if volume0 else None) - ((GDX_volume180 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore180_volume = (net180_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net180_volume is not None else None
           
            if zscore180_price is not None and zscore180_price > threshold and zscore180_volume is not None and zscore180_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '180', day180_date, f"{zscore180_price:.2f}", f"{zscore180_volume:.2f}"])

            close210 = closes[143] if len(closes) > 143 else closes[-1]
            close210_return = (close210 - close0) / close0 if close0 else None
            GDX_close210 = GDX[143]['c'] if len(GDX) > 143 else GDX[-1]['c'] if len(GDX) > 0 else None          
            GDX_close210_return = (GDX_close210 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net210_close = close210_return - GDX_close210_return if close210_return is not None and GDX_close210_return is not None else None
            zscore210_price = (net210_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net210_close is not None else None
           
            volume210 = volumes[143] if len(volumes) > 143 else volumes[-1]
            volume210_return = (volume210 - volume0) / volume0 if volume0 else None
            GDX_volume210 = GDX[143]['v'] if len(GDX) > 143 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume210_return = (GDX_volume210 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net210_volume = ((volume210 - volume0) / volume0 if volume0 else None) - ((GDX_volume210 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore210_volume = (net210_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net210_volume is not None else None
           
            if zscore210_price is not None and zscore210_price > threshold and zscore210_volume is not None and zscore210_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '210', day210_date, f"{zscore210_price:.2f}", f"{zscore210_volume:.2f}"])

            close240 = closes[164] if len(closes) > 164 else closes[-1]
            close240_return = (close240 - close0) / close0 if close0 else None
            GDX_close240 = GDX[164]['c'] if len(GDX) > 164 else GDX[-1]['c'] if len(GDX) > 0 else None  
            GDX_close240_return = (GDX_close240 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net240_close = close240_return - GDX_close240_return if close240_return is not None and GDX_close240_return is not None else None
            zscore240_price = (net240_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net240_close is not None else None
            
            volume240 = volumes[164] if len(volumes) > 164 else volumes[-1]
            volume240_return = (volume240 - volume0) / volume0 if volume0 else None
            GDX_volume240 = GDX[164]['v'] if len(GDX) > 164 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume240_return = (GDX_volume240 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net240_volume = ((volume240 - volume0) / volume0 if volume0 else None) - ((GDX_volume240 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore240_volume = (net240_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net240_volume is not None else None
           
            if zscore240_price is not None and zscore240_price > threshold and zscore240_volume is not None and zscore240_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '240', day240_date, f"{zscore240_price:.2f}", f"{zscore240_volume:.2f}"])

            close270 = closes[185] if len(closes) > 185 else closes[-1]
            close270_return = (close270 - close0) / close0 if close0 else None
            GDX_close270 = GDX[185]['c'] if len(GDX) > 185 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close270_return = (GDX_close270 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net270_close = close270_return - GDX_close270_return if close270_return is not None and GDX_close270_return is not None else None
            zscore270_price = (net270_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net270_close is not None else None
 
            volume270 = volumes[185] if len(volumes) > 185 else volumes[-1]
            volume270_return = (volume270 - volume0) / volume0 if volume0 else None
            GDX_volume270 = GDX[185]['v'] if len(GDX) > 185 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume270_return = (GDX_volume270 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net270_volume = ((volume270 - volume0) / volume0 if volume0 else None) - ((GDX_volume270 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore270_volume = (net270_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net270_volume is not None else None

            if zscore270_price is not None and zscore270_price > threshold and zscore270_volume is not None and zscore270_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '270', day270_date, f"{zscore270_price:.2f}", f"{zscore270_volume:.2f}"])

            close300 = closes[206] if len(closes) > 206 else closes[-1]
            close300_return = (close300 - close0) / close0 if close0 else None
            GDX_close300 = GDX[206]['c'] if len(GDX) > 206 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close300_return = (GDX_close300 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net300_close = close300_return - GDX_close300_return if close300_return is not None and GDX_close300_return is not None else None
            zscore300_price = (net300_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net300_close is not None else None
            
            volume300 = volumes[206] if len(volumes) > 206 else volumes[-1]
            volume300_return = (volume300 - volume0) / volume0 if volume0 else None
            GDX_volume300 = GDX[206]['v'] if len(GDX) > 206 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume300_return = (GDX_volume300 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net300_volume = ((volume300 - volume0) / volume0 if volume0 else None) - ((GDX_volume300 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore300_volume = (net300_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net300_volume is not None else None

            if zscore300_price is not None and zscore300_price > threshold and zscore300_volume is not None and zscore300_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '300', day300_date, f"{zscore300_price:.2f}", f"{zscore300_volume:.2f}"])

            close330 = closes[227] if len(closes) > 227 else closes[-1]
            close330_return = (close330 - close0) / close0 if close0 else None
            GDX_close330 = GDX[227]['c'] if len(GDX) > 227 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close330_return = (GDX_close330 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net330_close = close330_return - GDX_close330_return if close330_return is not None and GDX_close330_return is not None else None
            zscore330_price = (net330_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net330_close is not None else None
            
            volume330 = volumes[227] if len(volumes) > 227 else volumes[-1]
            volume330_return = (volume330 - volume0) / volume0 if volume0 else None
            GDX_volume330 = GDX[227]['v'] if len(GDX) > 227 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume330_return = (GDX_volume330 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net330_volume = ((volume330 - volume0) / volume0 if volume0 else None) - ((GDX_volume330 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore330_volume = (net330_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net330_volume is not None else None

            if zscore330_price is not None and zscore330_price > threshold and zscore330_volume is not None and zscore330_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '330', day330_date, f"{zscore330_price:.2f}", f"{zscore330_volume:.2f}"])

            close360 = closes[248] if len(closes) > 248 else closes[-1]     
            close360_return = (close360 - close0) / close0 if close0 else None
            GDX_close360 = GDX[248]['c'] if len(GDX) > 248 else GDX[-1]['c'] if len(GDX) > 0 else None
            GDX_close360_return = (GDX_close360 - GDX_close0) / GDX_close0 if GDX_close0 else None
            net360_close = close360_return - GDX_close360_return if close360_return is not None and GDX_close360_return is not None else None
            zscore360_price = (net360_close - sum(difference) / len(difference)) / (statistics.stdev(difference) if len(difference) > 1 else 1) if net360_close is not None else None
            
            volume360 = volumes[248] if len(volumes) > 248 else volumes[-1]
            volume360_return = (volume360 - volume0) / volume0 if volume0 else None
            GDX_volume360 = GDX[248]['v'] if len(GDX) > 248 else GDX[-1]['v'] if len(GDX) > 0 else None
            GDX_volume360_return = (GDX_volume360 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None
            net360_volume = ((volume360 - volume0) / volume0 if volume0 else None) - ((GDX_volume360 - GDX_volume0) / GDX_volume0 if GDX_volume0 else None)
            zscore360_volume = (net360_volume - sum(volumes) / len(volumes)) / (statistics.stdev(volumes) if len(volumes) > 1 else 1) if net360_volume is not None else None
            
            if zscore360_price is not None and zscore360_price > threshold and zscore360_volume is not None and zscore360_volume > vol_threshold:
                write_mode = 'a' if os.path.exists('Alphas.csv') else 'w'
                with open('Alphas.csv', mode=write_mode, newline='', encoding='utf-8') as alpha_both_file:
                    alpha_both_writer = csv.writer(alpha_both_file)
                    if write_mode == 'w':
                        alpha_both_writer.writerow(['Ticker', 'Days', 'Date', 'Price Z-score', 'Volume Z-score'])
                    alpha_both_writer.writerow([ticker, '360', day360_date, f"{zscore360_price:.2f}", f"{zscore360_volume:.2f}"])

    else:
        print(f"Market cap {j} for {ticker} is not greater than 50")

