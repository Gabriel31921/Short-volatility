import pickle
import pandas as pd
import os
import numpy as np

# We begin by retrieving our saved option pricing data—our careful record of past API calls.
# By using pickle, we can effortlessly rehydrate our Python objects without re-running expensive API requests.
directory_path = os.getenv("Short_Volatility_Path") + "/Seasonal/"
with open(os.path.join(directory_path, "Friday Options Data.pkl"), "rb") as file:
    Data_dict = pickle.load(file)

# Next, we load our SPY data, the backbone of our analysis.
# We focus on data from March 1, 2023, onward to capture the era in which our strategy is actively deployed.
F_SPY = pd.read_csv(os.path.join(directory_path, "Friday SPY data"))
F_SPY_2025 = F_SPY[F_SPY["Date"] >= "2023-03-01"].copy()

# We now enrich our SPY data with the option pricing details.
# Multiplying by 100 converts the prices to a more granular unit (e.g., cents instead of dollars),
# which is standard practice when dealing with options premiums.
F_SPY_2025["Call_prices"] = [100 * Data_dict["Call_prices"][i] for i in range(len(Data_dict["Call_prices"]))]
F_SPY_2025["Put_prices"] = [100 * Data_dict["Put_prices"][i] for i in range(len(Data_dict["Put_prices"]))]
# The total premium is the sum of what we received from both call and put options,
# offering a complete view of the income from our strategy.
F_SPY_2025["Premium"] = F_SPY_2025["Call_prices"] + F_SPY_2025["Put_prices"]

# Now we calculate the payoff.
# We use np.maximum to ensure that we only account for positive payoffs—mirroring the real-world behavior of options,
# where only in-the-money moves generate a payout.
F_SPY_2025['Payoff'] = np.maximum(F_SPY_2025['Next_Close'] - F_SPY_2025['Strike_Close'], 0) + \
                        np.maximum(F_SPY_2025['Strike_Close'] - F_SPY_2025['Next_Close'], 0)
# Multiplying by 100 keeps our units consistent with the premiums.
F_SPY_2025['Payoff'] *= 100
# Our profit or loss (PL) for each trade is then the premium collected minus the payout made.
F_SPY_2025['PL'] = F_SPY_2025["Premium"] - F_SPY_2025["Payoff"]

# With the enriched data now assembled, we save our merged DataFrame.
# This checkpoint preserves our work and allows us to revisit the analysis without reprocessing.
F_SPY_2025.to_csv(os.path.join(directory_path, "Friday SPY 2023 to 2025 data"), index=False)

# Time is of the essence in performance evaluation.
# We convert our 'Date' column to datetime so that our time-series analysis is accurate,
# then sort the records chronologically to properly trace the evolution of our equity curve.
F_SPY_2025["Date"] = pd.to_datetime(F_SPY_2025["Date"])
F_SPY_2025.sort_values("Date", inplace=True)
F_SPY_2025.reset_index(drop=True, inplace=True)

def compute_performance(df, initial_equity):
    """
    Computes key performance metrics for a portfolio given an initial equity.
    
    Parameters:
      df (DataFrame): The trade-level dataframe (must include Date and PL columns).
      initial_equity (float): The starting equity.
      
    Returns:
      dict: A dictionary containing performance metrics.
    """
    # We work on a copy of the data to safeguard the original record.
    data = df.copy()
    # Building an equity curve allows us to see how each trade influences our overall portfolio.
    data['Cumulative_PL'] = data['PL'].cumsum()
    data['Equity'] = initial_equity + data['Cumulative_PL']
    
    # Our final equity is the sum of the starting capital and the total profit/loss.
    # This leads us naturally to calculate the total return and the ROI.
    final_equity = data['Equity'].iloc[-1]
    total_return = final_equity - initial_equity
    ROI = total_return / initial_equity
    
    # To measure our strategy's annualized performance, we compute the Compound Annual Growth Rate (CAGR).
    # This metric smooths out the journey, providing a yearly rate that’s comparable to other investments.
    start_date = data["Date"].iloc[0]
    end_date = data["Date"].iloc[-1]
    years = (end_date - start_date).days / 365.25
    CAGR = (final_equity / initial_equity) ** (1 / years) - 1 if years > 0 else np.nan
    
    # Trade-level statistics reveal the underlying mechanics of our performance.
    # We separate winning trades from losing ones, calculate the win rate, and assess the average gains and losses.
    wins = data[data["PL"] > 0]
    losses = data[data["PL"] < 0]
    win_rate = len(wins) / len(data) if len(data) > 0 else np.nan
    avg_win = wins["PL"].mean() if not wins.empty else 0
    avg_loss = losses["PL"].mean() if not losses.empty else 0
    # The profit factor summarizes the risk/reward ratio by comparing total gains to total losses.
    profit_factor = wins["PL"].sum() / abs(losses["PL"].sum()) if losses["PL"].sum() != 0 else np.nan

    # Maximum drawdown tells the story of the worst period in our portfolio,
    # quantifying the largest drop from a peak to a subsequent trough.
    data['Roll_Max'] = data['Equity'].cummax()
    data['Drawdown'] = (data['Equity'] - data['Roll_Max']) / data['Roll_Max']
    max_drawdown = data['Drawdown'].min()
    
    # To understand risk-adjusted returns, we approximate the Sharpe Ratio.
    # By examining the volatility of trade-to-trade returns, we gauge whether our gains justify the risk taken.
    data['Trade_Return'] = data['Equity'].pct_change()
    if data['Trade_Return'].std() != 0 and len(data) > 1:
        sharpe_ratio = (data['Trade_Return'].mean() / data['Trade_Return'].std()) * np.sqrt(52)
    else:
        sharpe_ratio = np.nan

    # Our performance dictionary encapsulates the entire narrative of our trading journey.
    return {
        "Final Equity": final_equity,
        "Total Return ($)": total_return,
        "ROI": ROI,
        "CAGR": CAGR,
        "Win Rate": win_rate,
        "Average Win": avg_win,
        "Average Loss": avg_loss,
        "Profit Factor": profit_factor,
        "Max Drawdown": max_drawdown,
        "Sharpe Ratio": sharpe_ratio,
        "Number of Trades": len(data)
    }

# With our performance engine defined, we now compute our strategy's performance assuming a starting equity of $50,000.
metrics_50k = compute_performance(F_SPY_2025, 50000)

# To make our output more accessible, we define a helper function that formats decimal values as percentages.
def format_percentage(value, decimals=2):
    return f"{value * 100:.{decimals}f}%"

# Finally, we walk through our performance metrics and display them.
# For inherently percentage-based metrics like ROI and CAGR, we use our formatter to convert decimals into percentage terms.
for key, value in metrics_50k.items():
    if key in ["ROI", "CAGR", "Win Rate", "Max Drawdown"]:
        print(f"{key}: {format_percentage(value)}")
    elif isinstance(value, float):
        print(f"{key}: {value:.2f}")
    else:
        print(f"{key}: {value}")
