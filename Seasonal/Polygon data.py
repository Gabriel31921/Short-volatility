import requests
import os
import pandas as pd
import numpy as np
import yaml
import time
from datetime import timedelta

directory_path = os.getenv("Short_Volatility_Path") # Personal path for data storage.
directory_path = directory_path + "/Seasonal/"
Config_path = os.path.join(directory_path, "config.yaml")
with open(Config_path, "r") as file:
    config = yaml.safe_load(file)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY") # Environmental variable for API key, keeping it secure.

url = config["api"]["base_url"]
options_contracts_endpoint = config["api"]["endpoints"]["options_contracts"]  # Endpoint for option contracts.
Daily_OC = config["api"]["endpoints"]["daily_oc"]  # Endpoint for daily open/close data.
rate_limit = config["api"]["rate_limit_per_minute"]
headers = {"Authorization" : f"{POLYGON_API_KEY}"}  # Authorization header for API requests.

F_SPY = pd.read_csv(directory_path + "Friday SPY data")
F_SPY_2025 = F_SPY[F_SPY["Date"] >= "2023-03-01"]

Dates = F_SPY_2025["Date"].to_list()
Strikes = F_SPY_2025["Strike_Close"].to_list()
Expiration_dates = F_SPY_2025["Next_Date"].to_list()
Calls = []
Puts = []
Call_prices = []
Put_prices = []

full_url = url + options_contracts_endpoint

x = 0
for i in range(len(Dates)):
    params = {
        "underlying_ticker": "SPY",
        "contract_type": "call",
        "expiration_date": Expiration_dates[i],
        "strike_price": Strikes[i],
        "expired": "true",
        "sort": "expiration_date" 
    }

    response = requests.get(full_url, headers=headers, params=params)
    x += 1
    if x == 5:
        time.sleep(60)
        x = 0

    if response.status_code == 200:
        response = response.json()
        results = response.get("results", [])
        if results:
            print(results)
            Call_ticker = results[0]["ticker"]
            Calls.append(Call_ticker)
        else:
            print(f"No results for Call: {params}")  # Log if no results are found.
    else:
        print(f"Error in Call request: {response.status_code} - {response.text}")  # Log API errors.
    

x = 0 
for i in range(len(Dates)):
    params = {
        "underlying_ticker": "SPY",
        "contract_type": "put",
        "expiration_date": Expiration_dates[i],
        "strike_price": Strikes[i],
        "expired": "true",
        "sort": "expiration_date" 
    }

    response = requests.get(full_url, headers=headers, params=params)
    x += 1
    if x == 5:
        time.sleep(60)
        x = 0

    if response.status_code == 200:
        response = response.json()
        results = response.get("results", [])
        if results:
            print(results)
            Put_ticker = results[0]["ticker"]
            Puts.append(Put_ticker)
        else:
            print(f"No results for Put: {params}")  # Log if no results are found.
    else:
        print(f"Error in Put request: {response.status_code} - {response.text}")  # Log API errors.
    

x = 0
for i in range(len(Calls)):
    ticker = Calls[i]
    date = Dates[i]
    full_url = f"{url}{Daily_OC}{ticker}/{date}"

    params = {"adjusted": True}

    response = requests.get(full_url, headers=headers, params=params)
    x += 1
    if x == 5:
        time.sleep(60)
        x = 0
    if response.status_code == 200:
        response = response.json()
        if response:
            Call_price = response["close"]
            Call_prices.append(Call_price)
            print(response)
        else:
            print(f"No results for {ticker}: {params}")  # Log if no results are found.
    else:
        print(f"Error in {ticker} request: {response.status_code} - {response.text}")  # Log API errors.

    

x = 0
for i in range(len(Puts)):
    ticker = Puts[i]
    date = Dates[i]
    full_url = f"{url}{Daily_OC}{ticker}/{date}"

    params = {"adjusted": True}

    response = requests.get(full_url, headers=headers, params=params)
    x += 1
    if x == 5:
        time.sleep(60)
        x = 0
    if response.status_code == 200:
        response = response.json()
        if response:
            Put_price = response["close"]
            Put_prices.append(Put_price)
            print(response)
        else:
            print(f"No results for {ticker}: {params}")  # Log if no results are found.
    else:
        print(f"Error in {ticker} request: {response.status_code} - {response.text}")  # Log API errors.

F_SPY_2025["Calls_prices"] = Call_prices
F_SPY_2025["Puts_prices"] = Put_prices

F_SPY_2025["Premium"] = F_SPY_2025["Calls_prices"] + F_SPY_2025["Puts_prices"]
F_SPY_2025['Payoff'] = np.maximum(F_SPY_2025['Future_Close'] - F_SPY_2025['Strike_Close'], 0) + \
        np.maximum(F_SPY_2025['Strike_Close'] - F_SPY_2025['Next_Close'], 0)  # Payoff from the straddle.
F_SPY_2025['PL'] = F_SPY_2025['Premium'] - F_SPY_2025['Payoff']  # P&L for each straddle position.

F_SPY_2025.to_excel(directory_path + "Friday SPY 2023 data")

PL = F_SPY_2025["PL"].to_list()
Final_PL = sum(PL)

print(f"Result of the short straddles on Friday through Monday 3DTE: {Final_PL}")
