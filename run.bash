#! /bin/bash
# Run the AlphaTicker application

./clean.bash

python3 convert.py PMET\ Data.xlsx output_data.csv
python3 read_csv.py output_data.csv
python3 PMET_CN.py
python3 ship_xslx.py
python3 ship_xslx_vol.py