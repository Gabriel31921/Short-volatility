import os
import time
import json
import pickle
import requests
import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime, timedelta

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY") #Enviromental variable, you know, security and things...
Path = os.getenv("Short_Volatility_Path") #Personal path...
Path = os.path.join(Path, "ETF_filtered.pkl")

with open(Path, 'rb') as f:
    ETF_filtered = pickle.load(f)

ETF_filtered_2024 = {}

for ticker, df in ETF_filtered.items():
    df = df.reset_index()
    # Filter for rows where the year is 2024
    df_filtered = df[df["Date"].dt.year == 2024]
    # Update the new dictionary
    ETF_filtered_2024[ticker] = df_filtered.copy()
    
print(ETF_filtered_2024['SPY'])

### To do:
# Call the API of Polygon.io. The parameter of expiration date will be equal to 7 days later than the Date in each row of df_filtered.
# The paremeter of strike will be equal to the nearest integer of Close.
# Do this for both Puts and Calls.
# Only one option should be the response, because is 1 strike in 1 expiration date.
# Get the ticker of said option.
# 
# Call again the API, this time, the Daily Open/Close. This endpoint need special attention, still to get the url.
# Get the Close and the Volume of the option.
# Do this for both Puts and Calls.
# 
# Calculate for each pair of prices, the PL from the position. Future_Close is in the df in ETF_filtered_2024.
# Do this for every signal in the year. Compute the total PL and EV. Graph such.
# Do this for every ETF in the list. 
# Get criteria for leaving ETFs out. 
# 
# Compute Sharpe, total returns... the usual suspects.
# Measure portfolio profitability.

url = "https://api.polygon.io" 
options_contracts_endpoint = "/v3/reference/options/contracts"

Daily_OC = "/v1/open-close/"

headers = {"Authorization" : f"{POLYGON_API_KEY}"}

from datetime import timedelta
import time

for ticker, data in ETF_filtered_2024.items():
    Calls = []
    Puts = []
    Calls_Price = []
    Puts_Price = []
    Calls_Volume = []
    Puts_Volume = []
    Strikes = []

    if ticker == 'SPY':  # Only process 'SPY'
        x = 0
        for row in range(len(data)):
            strike = round(data.iloc[row]['Close'])  # Close price as the strike
            expiration_date = (data.iloc[row]['Date'] + timedelta(days=7)).strftime('%Y-%m-%d')
            Strikes.append(strike)

            # API call for Call options
            params = {
                "underlying_ticker": ticker,
                "contract_type": "call",
                "expiration_date": expiration_date,
                "strike_price": strike,
                "expired": "true",
                "limit": 10,
                "sort": "expiration_date",
            }
            full_url = url + options_contracts_endpoint
            response = requests.get(full_url, headers=headers, params=params)

            if response.status_code == 200:
                response = response.json()
                results = response.get("results", [])
                if results:
                    Call_ticker = results[0]['ticker']
                    Calls.append(Call_ticker)
                else:
                    print(f"No results for Call: {params}")
                    Calls.append(None)  # Append None if no result is found
            else:
                print(f"Error in Call request: {response.status_code} - {response.text}")
                Calls.append(None)

            # Rate limiting: Sleep after every 5 requests
            x += 1
            if x == 5:
                x = 0
                time.sleep(60)

            # API call for Put options
            params = {
                "underlying_ticker": ticker,
                "contract_type": "put",
                "expiration_date": expiration_date,
                "strike_price": strike,
                "expired": "true",
                "limit": 10,
                "sort": "expiration_date",
            }
            response = requests.get(full_url, headers=headers, params=params)

            if response.status_code == 200:
                response_data = response.json()
                results = response_data.get("results", [])
                if results:
                    Put_ticker = results[0]['ticker']
                    Puts.append(Put_ticker)
                else:
                    print(f"No results for Put: {params}")
                    Puts.append(None)  # Append None if no result is found
            else:
                print(f"Error in Put request: {response.status_code} - {response.text}")
                Puts.append(None)

            # Rate limiting: Sleep after every 5 requests
            x += 1
            if x == 5:
                x = 0
                time.sleep(60)

        # Add Call and Put tickers to the dataset
        data['Call_ticker'] = Calls
        data['Put_ticker'] = Puts
        data['Strike'] = Strikes

        data.dropna(inplace=True)
        print(data)
        time.sleep(60)

        def fetch_option_data(ticker, expiration_date, option_type, url, headers):
            """Fetch option data for a specific ticker and expiration date."""
            full_url = f"{url}{Daily_OC}{ticker}/{expiration_date}"
            params = {"adjusted": True}

            try:
                response = requests.get(full_url, headers=headers, params=params)
                if response.status_code == 200:
                    response = response.json()
                    if response:
                        return response.get("close"), response.get("volume")  # Return Close price and Volume
                    else:
                        print(f"No results for {option_type}: {params}")
                        return None, None
                else:
                    print(f"Error in {option_type} request: {response.status_code} - {response.text}")
                    return None, None
            except Exception as e:
                print(f"Exception in {option_type} request: {e}")
                return None, None
        
        x = 0
        time.sleep(60)
        for row in range(len(data)):
            # Extract basic info
            call_ticker = data.iloc[row]["Call_ticker"]
            put_ticker = data.iloc[row]["Put_ticker"]
            date = (data.iloc[row]["Date"]).strftime("%Y-%m-%d")
            
            x += 1
            if x == 5:
                time.sleep(60)
                x = 0
            # Fetch Call data
            call_price, call_volume = fetch_option_data(call_ticker, date, "Call", url, headers)
            Calls_Price.append(call_price)
            Calls_Volume.append(call_volume)
            
            x += 1
            if x == 5:
                time.sleep(60)
                x = 0
            # Fetch Put data
            put_price, put_volume = fetch_option_data(put_ticker, date, "Put", url, headers)
            Puts_Price.append(put_price)
            Puts_Volume.append(put_volume)

        data['Call_Price'] = Calls_Price
        data['Call_Volume'] = Calls_Volume
        data['Put_Price'] = Puts_Price
        data['Put_Volume'] = Puts_Volume

        data['Premium'] = data['Call_Price'] + data['Put_Price']
        data['Payoff'] = np.maximum(data['Future_Close'] - data['Strike'], 0) + \
            np.maximum(data['Strike'] - data['Future_Close'], 0)
        data['PL'] = data['Premium'] - data['Payoff']

        PL = data['PL'].tolist()
        final_PL = sum(PL)
        print(data)
        print(f"Result of the strategy on SPY: {final_PL}")

        break

### To do:
# Make the loop work for every ETF in the list, get the column of "PL" in dictionaries containint as keys the ETFs and as values the "PL" list.
# With this lists evaluate which ETFs were profitable, by how much, calculate metrics (Sharpe, return...)
# Graph the path of every ETF strategy.
# Get criteria for combining mulitple ETFs into a portfolio.
# Aggregate the data and get portfolio metrics.
# 
# Check ways to improve the timing on VRP.

