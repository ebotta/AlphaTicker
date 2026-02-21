#! /bin/bash
# Run the AlphaTicker application

rm Alphas.csv
rm Alphas.xlsx
rm fails.csv
rm output_data.csv
rm tickers.csv


python3 convert.py PMET\ Data.xlsx output_data.csv
python3 read_csv.py output_data.csv
python3 PMET_CN.py
python3 ship_xslx.py
