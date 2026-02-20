import csv
import time
import requests

CN_tickers = []

def _get_field(item, keys):
    for k in keys:
        if isinstance(item, dict) and k in item:
            return item[k]
    return None

# Read tickers from CSV (safe indexing)
try:
    with open('tickers.csv', mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            print(f"Original line tokens: {row}")
            if not row:
                print("Empty row, skipping")
                continue
            print(f"Raw row: {row}")
            first = row[0].strip() if len(row) >= 1 else ''
            second = row[1].strip().upper() if len(row) >= 2 else ''
            fourth = row[3].strip() if len(row) >= 4 else ''
            print(f"First token: {first}")
            print(f"Second token: {second}")
            print(f"Fourth token: {fourth}")
            if second == 'CN':
                print("This is a Canadian ticker.")
                CN_tickers.append(first + "," + fourth)
            print("-" * 20)

except FileNotFoundError:
    print("Error: 'tickers.csv' not found.")
except Exception as e:
    print(f"An error occurred while reading CSV: {e}")

print(f"Canadian Tickers: {CN_tickers}")
print("-" * 20)


# If no CN tickers found, keep a default example ticker
if not CN_tickers:
    CN_tickers = ['AEM']

headers = {}

for ticker in CN_tickers:
    print(f"Fetching data for: {ticker.split(',')[0]} in {ticker.split(',')[1]}")
    if ticker.split(',')[1] == "Toronto":
        CN_SE = "TO"
        print(f"Determined exchange code: {CN_SE} for ticker: {ticker.split(',')[0]}")
        
    elif ticker.split(',')[1] == "Venture":
        CN_SE = "V"
        print(f"Determined exchange code: {CN_SE} for ticker: {ticker.split(',')[0]}")
        
    elif ticker.split(',')[1] == "NEO-L":   
        CN_SE = "NEO"
        print(f"Determined exchange code: {CN_SE} for ticker: {ticker.split(',')[0]}")
        
    elif ticker.split(',')[1] == "Canadian":
        print ("This is a Canadian ticker but no exchange specified, skipping.")
        continue

    url = f"https://eodhd.com/api/fundamentals/{ticker.split(',')[0]}.{CN_SE}?filter=Highlights::MarketCapitalizationMln&api_token=6996093bcbc331.27702836&fmt=json"
    print(f"Constructed URL: {url}")

    try:
        resp = requests.get(url, headers=headers, timeout=15)
    except Exception as e:
        # print(f"Request failed for fundamentals of {ticker}: {e}")
        continue
    if not resp.ok:
        # print(f"Non-OK response for fundamentals of {ticker}: {resp.status_code}")
        with open('fails.csv', 'a', newline='') as out_file:
            out_ticker = csv.writer(out_file)
            out_ticker.writerow([ticker])
        continue
        
    try:        j = resp.json()
    except Exception as e:
        # print(f"Invalid JSON for fundamentals of {ticker}: {e}")
        continue
    # print(f"MarketCapitalizationMln for {ticker}: {j}")
    # print("-" * 20)

    if str(j).lower() == "na":
        # print(f"Market cap for {ticker} is not available.")
        continue

    if int(j) > 50:
        # print(f"Market cap {j} is greater than 50, fetching time series data...")
        url = f"https://eodhd.com/api/eod/{ticker.split(',')[0]}.{CN_SE}?api_token=6996093bcbc331.27702836&fmt=json&period=d&from=2026-01-20"
        


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

            # Access the headers directly
            remaining = resp.headers.get("X-RateLimit-Remaining")
            limit = resp.headers.get("X-RateLimit-Limit")

            # print(f"Requests remaining this minute: {remaining} / {limit}")
            if int(remaining) < 50:
                print("Approaching rate limit, sleeping for 60 seconds...")
                time.sleep(.1)
                print("slept for 0.1 seconds to respect rate limits")
                
        # print(f"Extracted {len(extracted)} data points for {ticker}")

        if not extracted:
            print("No usable time-series data found for this ticker.")
            print("-" * 20)
            continue

        dates = [item['d'] for item in extracted]
        closes = [item['c'] for item in extracted]
        volumes = [item['v'] for item in extracted]

        # print (f"Dates: {dates}")
        # print (f"Closes: {closes}")
        # print (f"Volumes: {volumes}")

        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        # print(f"Average Volume: {avg_volume}")

        # print("-" * 20)

        if avg_volume > 100000:    
            # pulling GDX for comparison
            url = "https://eodhd.com/api/eod/GDX.US?api_token=6996093bcbc331.27702836&fmt=json&period=d&from=2026-02-19"

            payload = {}
            headers = {}

            response = requests.request("GET", url, headers=headers, data=payload)

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
            
                print (f"GDX Closes: {[item['c'] for item in GDX]}")
                GDX_gain = (GDX[0]['c'] - GDX[-1]['c']) / GDX[-1]['c'] if len(GDX) >= 2 else 0
                ticker_gain = (closes[0] - closes[-1]) / closes[-1] if len(closes) >= 2 else 0
                print(f"GDX Gain: {GDX_gain:.2%}")
                print(f"{ticker} Gain: {ticker_gain:.2%}")
                if ticker_gain > GDX_gain:
                    print(f"{ticker} has outperformed GDX in the last 30 days.")    
    
                    with open('Alphas.csv', 'a', newline='') as out_file:
                        out_ticker = csv.writer(out_file)
                        out_ticker.writerow([f"{ticker.split(',')[0]} in the {CN_SE} exchange has outperformed GDX in the last 30 days by {ticker_gain - GDX_gain:.2%}"])

    else:
        print(f"Market cap {j} is not greater than 50")

