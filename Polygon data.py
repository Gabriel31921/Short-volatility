import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import datetime 
from datetime import datetime
import os
import requests
import pickle

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY") #Enviromental variable, you know, security and things...
Path = os.getenv("Short_Volatility_Path") #Personal path...
Path = os.path.join(Path, "ETF_filtered.pkl")

with open(Path, 'rb') as f:
    ETF_filtered = pickle.load(f)

url = "https://api.polygon.io" 
options_contracts_endpoint = "/v3/reference/options/contracts"

headers = {"Authorization" : f"{POLYGON_API_KEY}"}

params = {
    "underlying_ticker": "SPY",
    "contract_type": "call",
    "exercise_style" : "european",
    "expiration_date.gte": "2024-01-01",
    "strike_price.gt": 460,
    "strike_price.lt": 620,
    "expired": "true",
    "limit": 1000,
    "sort": "expiration_date",
}

full_url = url + options_contracts_endpoint

response = requests.get(full_url, headers=headers, params=params)
response_data = response.json()

results = response_data["results"]

ETF_filtered_2024 = {}

for ticker, df in ETF_filtered.items():
    df = df.reset_index()
    # Filter for rows where the year is 2024
    df_filtered = df[df["Date"].dt.year == 2024]
    # Update the new dictionary
    ETF_filtered_2024[ticker] = df_filtered
    
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
 