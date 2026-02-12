import csv
import requests
#import jq

CN_tickers = []

# Open the file and process it line by line
try:
    # Use 'newline='' parameter for consistent behavior across platforms
    with open('tickers.csv', mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        
        # Iterate over each row in the CSV file
        for row in reader:
            # 'row' is a list of strings (the tokens for that line)
            print(f"Original line tokens: {row}")
            
            # You can access individual tokens by index
            # For example, to print the first token:
            if row: # Check if the row is not empty
                print(f"First token: {row[0]}")
                print(f"Second token: {row[1]}")
                if row[1] == "CN":
                    print("This is a Canadian ticker.")
                    CN_tickers.append(row[0])
                

            
            print("-" * 20) # Separator for clarity

except FileNotFoundError:
    print(f"Error: The file 'your_file.csv' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")

print(f"Canadian Tickers: {CN_tickers}")
print("-" * 20) # Separator for clarity

# this is where the APIs are flexed
# this is only for QQQ for now, but we can loop through the CN_tickers list later to get data for all of them 

url = "https://api.finazon.io/latest/rivium/rivium_ca/time_series?interval=1mo&outputsize=12&timezone=UTC&apikey=b46637d673ff4f36b60159b2492a8acfxm&ticker=QQQ"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
print("-" * 20) # Separator for clarity

#result = jq('.data[] | {date: .date, open: .open, high: .high, low: .low, close: .close}').transform(response.text)


# Parse the JSON response into a Python dictionary/list
data = response.json()
print(data)
print("-" * 20) # Separator for clarity

# Extract specific data (example assumes a structure like: {"results": [{"name": "John", ...}]})

# Assuming your list is named 'data'

extracted_data = [{'d': item['d'], 'c': item['c'], 'v': item['v']} for item in data]
print(extracted_data)
print("-" * 20) # Separator for clarity

# calculate Market Cap = Price * Volume
dates = [item['d'] for item in extracted_data]
print(dates)
print("-" * 20) # Separator for clarity

market_caps = [item['c'] * item['v'] for item in extracted_data]
print(market_caps)
print("-" * 20) # Separator for clarity

# start playing with the data, maybe calculate some moving averages or something?
# this is just an example of calculating the difference in market cap between each month, we can do more complex stuff later
deltas = [market_caps[i] - market_caps[i-1] for i in range(1, len(market_caps))]
print(deltas)
print("-" * 20) # Separator for clarity

