## --- get_all_ticker_data.py --- ##
from get_stock_metadata import getStockMetadata
from get_stock_bars import fetch_bars_raw
from functools import reduce
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

# --- Config --- #
tickers_path = "ticker_list.txt"

# --- Load tickers --- #
tickers = []
with open(tickers_path,"r") as file:
    for line in file:
        for ticker in line.strip().split(","):
            tickers.append(ticker)

# --- Get dim and fact data for all tickers --- #
stockdim_dfs = []
stockfact_15min_dfs = []
stockfact_1day_dfs = []

for ticker in tickers:
    stockdim_df_cleaned = getStockMetadata(ticker)
    stockfact_15min_df_cleaned = fetch_bars_raw(ticker=ticker,timeframe=TimeFrame(15,TimeFrameUnit.Minute),start="2022-01-01",end="2022-02-02")
    stockfact_1day_df_cleaned = fetch_bars_raw(ticker=ticker,timeframe=TimeFrame.Day,start="2022-01-01",end="2022-02-02")

    stockdim_dfs.append(stockdim_df_cleaned)
    stockfact_15min_dfs.append(stockfact_15min_df_cleaned)
    stockfact_1day_dfs.append(stockfact_1day_df_cleaned)

# --- Combine all data --- #
final_stockdim_df = reduce(lambda df1,df2:df1.unionByName(df2),stockdim_dfs)
final_stockfact15min_df = reduce(lambda df1,df2:df1.unionByName(df2),stockfact_15min_dfs)
final_stockfact1day_df = reduce(lambda df1,df2:df1.unionByName(df2),stockfact_1day_dfs)

# --- Write to temporary delta tables --- #





