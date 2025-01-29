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

### README
# Be carefull when running this code, the free polygon version offers 5 API calls per minute.
# Right now the loop is setted to break after the first iteration, this is, only with SPY data.
# If you want to loop over all the ETFs, this is gonna take time, a lot of time.
# So be carefull, we could be talking +10 hours at a rate of 5 API calls per minute.

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY") # Environmental variable for API key, keeping it secure.
Path = os.getenv("Short_Volatility_Path") # Personal path for data storage.
Path = os.path.join(Path, "ETF_filtered.pkl") # Path to the filtered ETF data.

# Load the filtered ETF data from the pickle file.
# This contains the pre-processed data with signals, forecasted moves, etc.
with open(Path, 'rb') as f:
    ETF_filtered = pickle.load(f)

# Create a new dictionary to store data filtered for 2023 and beyond.
# This is to focus on recent data for more relevant analysis.
ETF_filtered_2023 = {}

# Filter the data to include only rows from 2023 onwards.
# This ensures we're working with the most recent market conditions.
for ticker, df in ETF_filtered.items():
    df = df.reset_index()  # Reset index for easier manipulation.
    df_filtered = df[df["Date"].dt.year >= 2023]  # Filter rows where the year is 2023 or later.
    ETF_filtered_2023[ticker] = df_filtered.copy()  # Store the filtered data in the new dictionary.
    # We use .copy() here to create a new, independent DataFrame object. Without .copy(), 
    # df_filtered would be a view of the original DataFrame, and modifications to it could 
    # inadvertently affect the original data. This ensures data integrity.

# Print the filtered data for SPY to verify the filtering process.
print(ETF_filtered_2023['SPY'])

### To do:
# Call the Polygon.io API to get option contracts for each signal.
# The expiration date will be 7 days after the signal date, and the strike will be the nearest integer to the Close price.
# This needs to be done for both Calls and Puts.
# Only one option contract should be selected per signal (1 strike, 1 expiration date).
# 
# Then, call the API again to get the Daily Open/Close data for the selected options.
# Extract the Close price and Volume for both Calls and Puts.
# 
# Calculate the P&L for each straddle position using the Future_Close price from the filtered data.
# Compute the total P&L and expected value (EV) for each ETF.
# Graph the results to visualize performance.
# 
# Apply this process to every ETF in the list.
# Establish criteria for excluding underperforming ETFs.
# 
# Compute performance metrics like Sharpe ratio, total returns, etc.
# Measure overall portfolio profitability.

# Define API endpoints and headers for Polygon.io.
url = "https://api.polygon.io" 
options_contracts_endpoint = "/v3/reference/options/contracts"  # Endpoint for option contracts.
Daily_OC = "/v1/open-close/"  # Endpoint for daily open/close data.

headers = {"Authorization" : f"{POLYGON_API_KEY}"}  # Authorization header for API requests.

# Dictionary to store portfolio P&L for each ETF.
Portfolio_PL = {}

# Loop through each ETF and its filtered data.
for ticker, data in ETF_filtered_2023.items():
    Calls = []  # List to store Call option tickers.
    Puts = []   # List to store Put option tickers.
    Calls_Price = []  # List to store Call option prices.
    Puts_Price = []   # List to store Put option prices.
    Calls_Volume = []  # List to store Call option volumes.
    Puts_Volume = []   # List to store Put option volumes.
    Strikes = []  # List to store strike prices.

    x = 0  # Counter for rate limiting.
    for row in range(len(data)):
        strike = round(data.iloc[row]['Close'])  # Round Close price to get the strike.
        # We round the Close price to the nearest integer because option strikes are typically 
        # set at whole numbers. This ensures we're working with realistic, tradable strikes.
        expiration_date = (data.iloc[row]['Date'] + timedelta(days=7)).strftime('%Y-%m-%d')  # Expiration date is 7 days after signal date.
        # We use 7 days as the expiration period to simulate weekly options, which are commonly 
        # traded and provide a good balance between liquidity and time decay.
        Strikes.append(strike)  # Store the strike price.

        # API call to get Call option contracts.
        params = {
            "underlying_ticker": ticker,
            "contract_type": "call",
            "expiration_date": expiration_date,
            "strike_price": strike,
            "expired": "true",  # Include expired contracts.
            "limit": 10,  # Limit results to 10.
            "sort": "expiration_date",  # Sort by expiration date.
        }
        full_url = url + options_contracts_endpoint
        response = requests.get(full_url, headers=headers, params=params)

        if response.status_code == 200:
            response = response.json()
            results = response.get("results", [])
            if results:
                Call_ticker = results[0]['ticker']  # Get the first matching Call option ticker.
                Calls.append(Call_ticker)
            else:
                print(f"No results for Call: {params}")  # Log if no results are found.
                Calls.append(None)  # Append None if no result is found.
        else:
            print(f"Error in Call request: {response.status_code} - {response.text}")  # Log API errors.
            Calls.append(None)

        # Rate limiting: Sleep after every 5 requests to avoid hitting API limits.
        # Polygon.io has rate limits, so we sleep for 60 seconds after every 5 requests 
        # to stay within the allowed number of requests per minute.
        x += 1
        if x == 5:
            x = 0
            time.sleep(60)

        # API call to get Put option contracts.
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
                Put_ticker = results[0]['ticker']  # Get the first matching Put option ticker.
                Puts.append(Put_ticker)
            else:
                print(f"No results for Put: {params}")  # Log if no results are found.
                Puts.append(None)  # Append None if no result is found.
        else:
            print(f"Error in Put request: {response.status_code} - {response.text}")  # Log API errors.
            Puts.append(None)

        # Rate limiting: Sleep after every 5 requests.
        x += 1
        if x == 5:
            x = 0
            time.sleep(60)

    # Add Call and Put tickers, and strike prices to the dataset.
    data['Call_ticker'] = Calls
    data['Put_ticker'] = Puts
    data['Strike'] = Strikes

    # Drop rows with missing values.
    # We use inplace=True to modify the DataFrame directly instead of creating a new one.
    # Using df = df.dropna() would create a new DataFrame, which is less memory efficient 
    # and can lead to confusion if the variable name is reused.
    data.dropna(inplace=True)  # Drop rows with missing values.

    print(data)  # Print the updated dataset for verification.
    time.sleep(60)  # Sleep to avoid API rate limits.

    # Function to fetch option data (Close price and Volume) for a specific ticker and date.
    def fetch_option_data(ticker, expiration_date, option_type, url, headers):
        """Fetch option data for a specific ticker and expiration date."""
        full_url = f"{url}{Daily_OC}{ticker}/{expiration_date}"
        params = {"adjusted": True}  # Use adjusted prices.

        try:
            response = requests.get(full_url, headers=headers, params=params)
            if response.status_code == 200:
                response = response.json()
                if response:
                    return response.get("close"), response.get("volume")  # Return Close price and Volume.
                else:
                    print(f"No results for {option_type}: {params}")  # Log if no results are found.
                    return None, None
            else:
                print(f"Error in {option_type} request: {response.status_code} - {response.text}")  # Log API errors.
                return None, None
        except Exception as e:
            print(f"Exception in {option_type} request: {e}")  # Log exceptions.
            return None, None
    
    x = 0
    time.sleep(60)  # Sleep to avoid API rate limits.
    for row in range(len(data)):
        # Extract basic info for the current row.
        call_ticker = data.iloc[row]["Call_ticker"]
        put_ticker = data.iloc[row]["Put_ticker"]
        date = (data.iloc[row]["Date"]).strftime("%Y-%m-%d")  # Format date for API request.
            
        x += 1
        if x == 5:
            time.sleep(60)  # Sleep after every 5 requests.
            x = 0
        # Fetch Call option data.
        call_price, call_volume = fetch_option_data(call_ticker, date, "Call", url, headers)
        Calls_Price.append(call_price)
        Calls_Volume.append(call_volume)
            
        x += 1
        if x == 5:
            time.sleep(60)  # Sleep after every 5 requests.
            x = 0
        # Fetch Put option data.
        put_price, put_volume = fetch_option_data(put_ticker, date, "Put", url, headers)
        Puts_Price.append(put_price)
        Puts_Volume.append(put_volume)

    # Add Call and Put prices/volumes to the dataset.
    data['Call_Price'] = Calls_Price
    data['Call_Volume'] = Calls_Volume
    data['Put_Price'] = Puts_Price
    data['Put_Volume'] = Puts_Volume

    # Calculate straddle premium and payoff.
    data['Premium'] = data['Call_Price'] + data['Put_Price']  # Total premium received.
    data['Payoff'] = np.maximum(data['Future_Close'] - data['Strike'], 0) + \
        np.maximum(data['Strike'] - data['Future_Close'], 0)  # Payoff from the straddle.
    data['PL'] = data['Premium'] - data['Payoff']  # P&L for each straddle position.

    # Calculate total P&L for the ETF.
    Premium = data['Premium'].tolist()
    Payoff = data['Payoff'].tolist()
    PL = data['PL'].tolist()
    final_PL = sum(PL)
    print(f"Result of the strategy on {ticker}: {final_PL}")  # Print the total P&L.

    # Store the P&L list in the portfolio dictionary.
    Portfolio_PL[ticker] = {
        "Premium" : Premium,
        "Payoff" : Payoff,
        "PL" : PL
    }
    break

Path = os.getenv("Short_Volatility_Path") # Personal path for data storage.
Path = os.path.join(Path, "Portfolio_PL.pkl") # Path to the Portfolio_PL data.

with open(Path, 'wb') as f:
    pickle.dump(Portfolio_PL, f)

### To do:
# Make the loop work for every ETF in the list, get the column of "PL" in dictionaries containing as keys the ETFs and as values the "PL" list.
# With these lists, evaluate which ETFs were profitable, by how much, and calculate metrics (Sharpe, return, etc.).
# Graph the performance path of every ETF strategy.
# Establish criteria for combining multiple ETFs into a portfolio.
# Aggregate the data and compute portfolio-level metrics.
# 
# Explore ways to improve the timing of the Volatility Risk Premium (VRP) strategy.
#
# All these will be done in another file.

