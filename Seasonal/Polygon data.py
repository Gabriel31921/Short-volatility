import requests
import os
import pandas as pd
import numpy as np
import yaml
import time
import pickle
from datetime import timedelta

# We start by pinpointing the secret location where our data lives.
# The environment variable "Short_Volatility_Path" ensures that our sensitive file paths remain private.
directory_path = os.getenv("Short_Volatility_Path")
# To keep our seasonal analyses neatly organized, we append the "Seasonal" subdirectory.
directory_path = directory_path + "/Seasonal/"

# Our configuration settings are stored externally in a YAML file.
# This allows us to adjust API endpoints, rate limits, and other parameters without changing our code.
Config_path = os.path.join(directory_path, "config.yaml")
with open(Config_path, "r") as file:
    config = yaml.safe_load(file)

# We retrieve our API key from an environment variable for security reasons.
# This practice keeps our credentials safe and out of our source code.
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Using our configuration, we extract the base URL and specific endpoints.
# The endpoints guide our requests: one for option contracts, another for daily open/close data.
url = config["api"]["base_url"]
options_contracts_endpoint = config["api"]["endpoints"]["options_contracts"]
Daily_OC = config["api"]["endpoints"]["daily_oc"]
# We also respect the API’s request-per-minute limitations by reading the rate limit from our config.
rate_limit = config["api"]["rate_limit_per_minute"]

# Our API requests need proper identification.
# Thus, we build an authorization header that quietly conveys our credentials.
headers = {"Authorization" : f"{POLYGON_API_KEY}"}

# Our journey continues as we load historical SPY data.
# This CSV file forms the foundation for our options strategy analysis.
F_SPY = pd.read_csv(directory_path + "Friday SPY data")
# We choose to focus on data from March 1, 2023, onward, where our strategy's dynamics truly unfold.
F_SPY_2025 = F_SPY[F_SPY["Date"] >= "2023-03-01"].copy()

# From our curated DataFrame, we extract key details:
# Dates tell us when each trade was signaled,
# Strike prices guide our option selection,
# And expiration dates reveal when these options come due.
Dates = F_SPY_2025["Date"].to_list()
Strikes = F_SPY_2025["Strike_Close"].to_list()
Expiration_dates = F_SPY_2025["Next_Date"].to_list()

# We initialize empty lists to capture the call and put option details.
# These lists will store the tickers and, later on, their closing prices.
Calls = []
Puts = []
Call_prices = []
Put_prices = []

# We combine our base URL with the endpoint for option contracts.
# This concatenation provides a complete address for our API calls.
full_url = url + options_contracts_endpoint

# Before diving into our API calls, we give the system a brief moment (60 seconds).
# This pause can help avoid triggering rate limits immediately upon starting our requests.
time.sleep(60)

# We introduce a simple counter, 'x', to keep track of our API requests.
# Respecting rate limits is paramount, and this counter helps us stay within bounds.
x = 0

# Now begins the first chapter of our API saga: fetching call option tickers.
# For each trading day, we construct a set of parameters that reflect our precise requirements—
# targeting SPY calls with the exact strike and expiration criteria.
for i in range(len(Dates)):
    params = {
        "underlying_ticker": "SPY",  # Our strategy revolves around the SPY ETF.
        "contract_type": "call",     # We start with the bullish call options.
        "expiration_date": Expiration_dates[i],
        "strike_price": Strikes[i],
        "expired": "true",           # We include expired contracts to gain a comprehensive view.
        "sort": "expiration_date"    # Sorting by expiration ensures consistency in results.
    }

    # We send our request to the API, carrying our carefully crafted parameters.
    response = requests.get(full_url, headers=headers, params=params)
    x += 1  # Each API call nudges our counter upward.
    if x == 5:
        # Every five requests, we pause—a respectful nod to the API's rate limitations.
        time.sleep(60)
        x = 0

    # Upon receiving a response, we first check for success.
    if response.status_code == 200:
        response = response.json()  # Convert the raw response into a navigable dictionary.
        results = response.get("results", [])
        if results:
            # When the stars align and we get valid data, we select the first call option ticker.
            print(results)
            Call_ticker = results[0]["ticker"]
            Calls.append(Call_ticker)
        else:
            # Should no results appear, we log the circumstance and carry on.
            print(f"No results for Call: {params}")
    else:
        # Any API hiccups are noted with a detailed error message for future debugging.
        print(f"Error in Call request: {response.status_code} - {response.text}")

# With our call options recorded, we turn our gaze to the put options.
# This loop mirrors the previous one, with the only difference being the option type.
for i in range(len(Dates)):
    params = {
        "underlying_ticker": "SPY",
        "contract_type": "put",      # Now, we focus on the defensive put options.
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
            print(f"No results for Put: {params}")
    else:
        print(f"Error in Put request: {response.status_code} - {response.text}")
    
# Having gathered the tickers, we now delve into pricing data for the call options.
# Each ticker is paired with its corresponding trading date, as we seek to capture the closing price.
for i in range(len(Calls)):
    ticker = Calls[i]
    date = Dates[i]
    # We craft a URL specific to the daily open/close data for the given ticker and date.
    full_url = f"{url}{Daily_OC}{ticker}/{date}"

    params = {"adjusted": True}  # Adjustments are crucial to account for corporate actions or splits.

    response = requests.get(full_url, headers=headers, params=params)
    x += 1
    if x == 5:
        time.sleep(60)
        x = 0
    if response.status_code == 200:
        response = response.json()
        if response:
            # The 'close' price encapsulates the final market sentiment for the day.
            Call_price = response["close"]
            Call_prices.append(Call_price)
            print(response)
        else:
            print(f"No results for {ticker}: {params}")
    else:
        print(f"Error in {ticker} request: {response.status_code} - {response.text}")

# In a similar vein, we fetch the closing prices for our put options.
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
            print(f"No results for {ticker}: {params}")
    else:
        print(f"Error in {ticker} request: {response.status_code} - {response.text}")

# With our data now in hand, we consolidate our findings into a single dictionary.
# This packaging step makes it simple to store and later retrieve our curated dataset.
Data = {
    "Calls" : Calls,
    "Puts" : Puts,
    "Call_prices" : Call_prices,
    "Put_prices": Put_prices
}

# We persist our options data to disk using pickle—a fast, binary serialization method.
# This choice avoids unnecessary API calls on subsequent runs, saving time and resources.
with open(directory_path + "Friday Options Data.pkl", "wb") as file:
    pickle.dump(Data, file)

# Our final act is to weave the collected option prices back into our original SPY DataFrame.
# By merging this data, we enrich our historical record with real option performance.
F_SPY_2025["Call_prices"] = Call_prices
F_SPY_2025["Put_prices"] = Put_prices
# The "Premium" is simply the sum of both call and put prices, representing the total received.
F_SPY_2025["Premium"] = F_SPY_2025["Call_prices"] + F_SPY_2025["Put_prices"]

# We then calculate the option "Payoff". Using np.maximum ensures we only capture positive outcomes,
# reflecting the asymmetric nature of option payouts.
F_SPY_2025['Payoff'] = np.maximum(F_SPY_2025['Next_Close'] - F_SPY_2025['Strike_Close'], 0) + \
        np.maximum(F_SPY_2025['Strike_Close'] - F_SPY_2025['Next_Close'], 0)
        
# The profit/loss (PL) for each trade is defined as the premium received minus the actual payout.
# This metric is central to evaluating the effectiveness of our strategy.
F_SPY_2025['PL'] = F_SPY_2025["Premium"] - F_SPY_2025["Payoff"]

# With our data now fully integrated and our strategy's performance mapped out,
# we save the final DataFrame to disk for further analysis or reporting.
F_SPY_2025.to_csv(directory_path + "Friday SPY 2023 to 2025 data")
