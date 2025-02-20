import requests
import os
import yaml
import datetime
import time

import pandas as pd

from datetime import timedelta, datetime

directory_path = os.getenv("Short_Volatility_Path")
directory_path = directory_path +"/Seasonal/BTC/"

config_path = os.path.join(directory_path, "config.yaml")
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

deribit_get_trades = config["deribit_api"]["get_trades"]
deribit_get_instruments = config["deribit_api"]["get_instruments"]

bitget_url = config["bitget_api"]["url"]
bitget_data = config["bitget_api"]["endpoints"]["historical_data"]
limit_per_second = config["bitget_api"]["limit"]

symbol = config["data"]["symbol"]
granularity = config["data"]["granularity"]
limit = config["data"]["limit"]

bitget_data = bitget_url + bitget_data

data_start = datetime.strptime("2020-01-01", "%Y-%m-%d")
data_end = datetime.strptime("2025-02-01", "%Y-%m-%d")
interval = timedelta(hours=limit)

data_timepoints = []
current_time = data_start
while current_time <= data_end:
    # Convert current_time to Unix milliseconds
    data_timepoints.append(int(current_time.timestamp() * 1000))
    current_time += interval

historical_data_time = []
historical_data_open = []
historical_data_high = []
historical_data_low = []
historical_data_close = []

requests_per_second = 0
for i in range(len(data_timepoints)):
    params = {
        "symbol": symbol,
        "granularity": granularity,
        "endTime": data_timepoints[i],
        "limit": limit
    }

    response = requests.get(url=bitget_data, params=params)
    response = response.json()
    data = response.get("data",[])
    for datapoint in data:
        historical_data_time.append(datapoint[0])
        historical_data_open.append(float(datapoint[1]))
        historical_data_high.append(float(datapoint[2]))
        historical_data_low.append(float(datapoint[3]))
        historical_data_close.append(float(datapoint[4]))
    requests_per_second += 1
    if requests_per_second == limit_per_second:
        time.sleep(1)
        requests_per_second = 0

Historical_data = {
    "Time" : historical_data_time,
    "Open" : historical_data_open,
    "High" : historical_data_high,
    "Low" : historical_data_low,
    "Close" : historical_data_close
}

Historical_data = pd.DataFrame(Historical_data)

Historical_data.to_csv(directory_path + "Historical BTC data 2020 - 2025")