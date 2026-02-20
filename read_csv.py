import csv

with open('output_data.csv', newline='') as in_file:
    readlines = csv.reader(in_file, delimiter=',')
    for line in readlines:
#        print(line[0])
        ticker=line[0].split(" ")
        SE_code=line[2].split(" ")
#        print(ticker[0])
        with open('tickers.csv', 'a', newline='') as out_file:
            out_ticker = csv.writer(out_file)
            out_ticker.writerow(ticker + SE_code)

