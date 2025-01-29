import datetime
import pickle
import os

import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt

from datetime import datetime

end = datetime.now().strftime('%Y-%m-%d')
etf_data = {}

### Important!!
# Somehow, somewhy, this code raises an error about columns not being there or ETF being delisted or something else.
# Don't mind the error, run it again. I don't know why it does that, but it does.
# Once you run it again, the code should work. Should.


tickers = ['SPY', 'QQQ', 'IWM', 'DIA', 'VOO', 'EEM', 'EFA', 'MDY', 'IJH', 'IJR',
        'XLE', 'XLF', 'XLK', 'XLV', 'XLY', 'XLP', 'XLI', 'XLU', 'XLB', 'XLRE',
        'VWO', 'FXI', 'EWY', 'EWJ', 'EWZ', 'EWW', 'EWG', 'INDA', 'FEZ', 'EWH',
        'TLT', 'LQD', 'HYG', 'JNK', 'GOVT', 'BND', 'TIP', 'IEF', 'SHY', 'SJNK',
        'GLD', 'SLV', 'USO', 'DBC', 'PLL', 'PPLT', 'CORN', 'WEAT', 'DBA', 'UCO',
        'VIXY', 'VXX', 'UVXY', 'SVXY', 'USMV', 'ACWV', 'TAIL', 'SPLV', 'JEPI', 'WTMF',
        'TQQQ', 'SQQQ', 'UPRO', 'SSO', 'SPXL', 'SPXS', 'URTY', 'TNA', 'TZA',
        'VIG', 'DVY', 'SCHD', 'SDY', 'HDV', 'VYM', 'NOBL', 'DGRW', 'PGX', 'SDIV',
        'ARKK', 'ARKW', 'ARKG', 'BOTZ', 'ICLN', 'SKYY', 'LIT', 'SOXX', 'SMH', 'CNRG',
        'BITO', 'XBTF', 'BTF', 'BLOK', 'BKCH',
        'ESGU', 'EFIV', 'ESGV', 'NULV', 'DMXF']

for idx, ticker in enumerate(tickers, start=1):
    etf_data[idx] = {
        "ticker" : ticker,
        "data" : yf.Ticker(ticker).history(start='2012-01-01', end=end, actions=True, auto_adjust=True) 
        #Actions is whether to include dividens and splits, it's True by default.
        #For easier manipulation, in case some ETFs have and adjusted close, the auto_adjust makes the Close being automatically adjusted.
    }
    #print(f"Fetching data for {ticker} (ID: {idx})...") #Just to show code is working...

#Structure of the "finished" dictionary:
#etf_data = {
#   1 : {
#       ticker : 'SPY',
#       data :  Dataframe
#   },
#   2 : ...
#}

vol_window = 5 #Number of periods for vol calculations.
mom_window = 5 #Number of periods for momentum calculations.
for_window = 7 #Number of periods for the forecasting.
mom_1 = 1 #First momentum thresold, in percentages of move.

x = 0 #This varible is for testing.

for index, etf_info in etf_data.items():
    data = etf_info['data'] #Here we extract the dataframe from the dictionary.
    data['Log_returns'] = np.log(data['Close'] / data['Close'].shift(1)) #Getting the Logarithmic returns.
    data['Volatility'] = data['Log_returns'].rolling(window=vol_window).std() #Volatility as standard deviation of Close on Close over "vol_window" days.
    Vol_20th = data['Volatility'].quantile(0.2)  #The 20 percent level of volatility.
    data['Signal_vol'] = data['Volatility'] <= Vol_20th #Signal if Voltility is below the thresold of 20 percent.
    data['Momentum'] = ((data['Close'] - data['Close'].shift(mom_window)) / data['Close'].shift(mom_window)) *100 #Get momentum as percentage change over "mom_window" days.
    data['Signal_mom_1'] = data['Momentum'].abs() <= mom_1 #Signal if momentum is below thresold desired

    data = data.dropna() #Dropping the NaN generated by shifting
    data = data.drop(columns=['Volume', 'Dividends', 'Stock Splits']) #Mostly no use for these columns 

    etf_info['data'] = data #Updating the dictionary

    ###
    # Up till now, what it's done is the creation of the "analysis" columns.
    # With these columns we have our criteria, vol lower than 20% historical vol and momentum less or equal than certain percentage.
    # Now, it's turn to do the "forecasting", I will take the days that fit the criteria, and see the forecast move for a certain period.
    # How will I determine the forecast is good or bad?
    # First we have to know that I'm taking the historical volatility, including periods not yet happened in the data. 
    # For a forecast of 2019, the volatility thresold includes vol from 2020, 2021...
    # So, with that in mind, I will look up the premium of an ATM straddle, get the percentages in which it would've made money in this circunstances,
    # and then apply that to the good/bad forecasting.

    data['Future_Close'] = data['Close'].shift(-for_window) #Creating a new column makes our future calculations easier.
    data['Forecast_Move'] = ((data['Future_Close'] - data['Close']) / data['Close']) * 100 #Calculating the future move as a percentage change.
    data['Forecast_Move_abs'] = data['Forecast_Move'].abs() #This is what we will be using for statistics.

    # Signal_1: Both Signal_vol and Signal_mom_1 must be True (1)
    data['Signal_1'] = ((data['Signal_vol']) & (data['Signal_mom_1'])).astype(int)

    etf_info['data'] = data #Updating the data (again)

    #if x == 0: #This lines are for testing.
    #    print(data['Momentum'].head())
    #    print(data[data['Signal_1'] == 1].tail(25))
    #    x += 1

results = [] #Start a list that will contain the statistics for forecasted moves.

for index, etf_info in etf_data.items():
    name = etf_info['ticker']
    data = etf_info['data'] #Same procedure as before.
    filtered_data = data[data['Signal_1'] == 1] #Filter the data so we can get only our interested part.

    if filtered_data.empty: #In case some ETF does not match the criteria we have selected, this gets called and gives us the name of such ETF for further testing.
        print(f'Etf number {index}, has no rows that match criteria. Investigatee further\n'
              f'ETF = {name}')
        continue

    mean = filtered_data['Forecast_Move_abs'].mean() 
    median = filtered_data['Forecast_Move_abs'].median()
    std = filtered_data['Forecast_Move_abs'].std()

    results.append({
        "ETF_Name": name,              #ETF ticker name
        "Mean_Forecast_Move": mean,  #Mean of Forecast_Move_asb
        "Median_Forecast_Move": median,  #Median of Forecast_Move_asb
        "Std_Forecast_Move": std     #Standard deviation of Forecast_Move_asb
    })

summary_table = pd.DataFrame(results)

pd.set_option('display.max_rows', None)  #Show all rows
pd.set_option('display.max_columns', None)  #Show all columns

#print(summary_table)
#summary_table.to_excel('Summary table.xlsx')

data = etf_data[1]['data'] #This is for the first ETF, could be any number between 1 and 99. The number is a manually setted index, it does not start in 0.
filtered_data = data[data['Signal_1'] == 1]

x = 1 #More tests :)
if x == 0:
    print(filtered_data.head())
    x+=1

print(f'The number of days of the initial data is of: {len(data)}\n'
      f'The number of possible signals inside said days is of: {len(filtered_data)}\n'
      f'Keep in mind both numbers may vary with the ETF selection.')

### I'm gonna practice a simple scenario with straddle prices as of 26/01 with SPY and the distribution of moves.
# This is just a small sandbox section to work out expected straddle payoffs. Nothing finalized here.

x = 1  # Testing variable again. Used for controlling execution of this segment.
if x == 0:
    def straddle_profit(move, asset):
        # Define premiums and strike prices for the straddle.
        # These are hardcoded for now, based on SPY prices on 26/01.
        premiums = {
            'P1': 4.51,  # Call premium
            'P2': 3.55   # Put premium
        }
        strike = 607  # The strike price is the same for both the call and the put.

        # Calculate the value of the asset after the move.
        # 'move' is in percentage terms, so we adjust it to get the new asset value.
        move_value = (move / 100 + 1) * asset

        # Calculate the profits for the call and the put options.
        # Calls: Positive difference between the move value and the strike.
        # Puts: Positive difference between the strike and the move value.
        call_profit = max(move_value - strike, 0)
        put_profit = max(strike - move_value, 0)

        # Total premiums for the straddle (both call and put costs combined).
        total_premiums = premiums['P1'] + premiums['P2']

        # Adjust the profits for contract size (100 shares per option).
        # Subtract the premiums to account for the initial cost of the straddle.
        profit = (-call_profit - put_profit + total_premiums) * 100

        return profit

    x += 1  # Prevents this block from running again unless x is reset.

    # Compute straddle profits for all forecasted moves in the dataset.
    # Using SPY's adjusted price as of 26/01 for this calculation (607.47).
    profits = [straddle_profit(filtered_data.iloc[i]['Forecast_Move'], 607.47) for i in range(len(filtered_data))]

    # Calculate the expected value (mean) of the straddle profits.
    # This gives an idea of whether the strategy is net profitable in this scenario.
    expected_value = np.mean(profits)

    print(f"Expected value of the SPY straddle: {expected_value:.2f}")
    # The expected value here should help answer whether the forecasted moves align with actual market pricing.

# Create a set of ETF names with "Mean_Forecast_Move" > "thresold" from the results list
mean_thresold = 2
excluded_etfs = {res["ETF_Name"] for res in results if res["Mean_Forecast_Move"] > mean_thresold}


# Filtering the dataset to create a dictionary of filtered ETFs for further analysis.
# This dictionary will only include relevant columns for the "Signal_1" data.
ETF_filtered = {}

for index, etf_info in etf_data.items():
    name = etf_info['ticker']  # Get the ticker name.
    data = etf_info['data']  # Get the corresponding dataframe for this ETF.

    # Skip ETFs that are in the exclusion set
    if name in excluded_etfs:
        print(f"Excluding ETF {name} due to high Mean_Forecast_Move (>{mean_thresold}).")
        continue

    # Filter rows where 'Signal_1' is True, i.e., matching the criteria set earlier.
    filtered_data = data[data['Signal_1'] == 1]

    # Keep only the relevant columns for this analysis: Date, Close, Future_Close, Forecast_Move.
    filtered_data = filtered_data.loc[:, filtered_data.columns.intersection(['Date', 'Close', 'Future_Close', 'Forecast_Move'])]

    # Add this filtered dataset to the dictionary, keyed by the ETF ticker.
    ETF_filtered[name] = filtered_data

# Display the first few rows of the filtered data for SPY as a sanity check.
print(ETF_filtered['SPY'].head())

# Save the filtered dataset to a pickle file for later use.
# The file path is fetched from an environment variable (personalized setup).
Path = os.getenv("Short_Volatility_Path")
Path = os.path.join(Path, "ETF_filtered.pkl")

# Uncomment this block if you want to save the data for future runs.
# This saves time by avoiding the need to re-run the entire analysis pipeline.
# with open(Path, 'wb') as f:
#     pickle.dump(ETF_filtered, f)
