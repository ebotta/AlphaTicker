#! /bin/bash
# Run the AlphaTicker application

./clean_forecast.bash

python3 CN_Alpha_forecast.py
python3 ship_Forecast_xslx.py
