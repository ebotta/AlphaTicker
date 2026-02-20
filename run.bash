#! /bin/bash
# Run the AlphaTicker application

python3 convert.py ../../Downloads/PMET\ Sector.xlsx output_data.csv
python3 read_csv.py output_data.csv
python3 PMET_CN.py

