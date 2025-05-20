## --- get_stock_bars.py --- ##
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from pyspark.sql import SparkSession,Row
from datetime import datetime
from config import get_alpaca_keys
from pyspark.sql.functions import col,pandas_udf,lit,from_utc_timestamp,hour,minute,expr,to_timestamp, date_format, concat_ws
import json
import pandas as pd

# --- Config --- #
#schema
schema_path = "fact_stock_schema.json"
# keys
keys = get_alpaca_keys()
api_key = keys["api_key"]
secret_key = keys["secret_key"]

# --- Initialize SparkSession --- #
spark = SparkSession.builder.appName("GetStockFacts").config("spark.sql.session.timeZone", "UTC").getOrCreate()

# --- UDFs for EMA calculation --- #
@pandas_udf("double")
def ema20(close: pd.Series) -> pd.Series:
    return close.ewm(span=20, adjust=False).mean()
@pandas_udf("double")
def ema50(close: pd.Series) -> pd.Series:
    return close.ewm(span=50, adjust=False).mean()


# --- Get bars for each ticker --- #
def fetch_bars_raw(ticker: str, timeframe:TimeFrame, start: str, end: str):
  
    client = StockHistoricalDataClient(api_key, secret_key)

    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")
    timeframe_str = str(timeframe)

    request_params = StockBarsRequest(
                        symbol_or_symbols=ticker,
                        timeframe=timeframe,
                        start=start,
                        end=end
                 )
    
    bars = client.get_stock_bars(request_params)
  
    # now convert into dataframe
    rows = [Row(**bar.__dict__) for bar in bars[ticker]]
    bars_df = spark.createDataFrame(rows)

    # load schema from external JSON file and parse using StructType
    from pyspark.sql.types import StructType

    with open(schema_path, 'r') as file:
        schema_json = json.load(file)

    schema = StructType.fromJson(schema_json)

    # get data for columns matching schema
    schema_columns = [field.name for field in schema.fields]
    columns_to_keep = [col for col in schema_columns  if col in bars_df.columns]

    # extract only columns metioned in schema

    bars_df_cleaned = bars_df.select(*columns_to_keep)

    '''transformations:
    -add timeframe
    -calculate and add 20-ema and 50-ema
    -convert timestamp from utc to est
    -change volume to full number from scientific notation
    -remove pre-market and post-market data
    -change timestamp for daily bars to start at market start
    '''

    bars_df_transformed = bars_df_cleaned.withColumn("timeframe",lit(timeframe_str))\
    .withColumn("ema20",ema20(bars_df_cleaned["close"]).cast("decimal(10,2)"))\
    .withColumn("ema50",ema50(bars_df_cleaned["close"]).cast("decimal(10,2)"))\
    .withColumn("timestamp",from_utc_timestamp(col("timestamp"), "America/New_York"))\
    .withColumn("volume",col("volume").cast("decimal(20,0)"))\
    .select(["symbol","timeframe","timestamp","low","high","open","close","volume","ema20","ema50"])

    # Add this only for daily timeframe
    if timeframe_str == "1Day":
        bars_df_transformed = bars_df_transformed.withColumn("timestamp",expr("timestamp + interval 9 hours 30 minutes"))
        
    # Get only market hours 
    if timeframe != TimeFrame.Day:
        bars_df_transformed = bars_df_transformed.filter(
            ((hour("timestamp") > 9) | ((hour("timestamp") == 9) & (minute("timestamp") >= 30))) &
            (hour("timestamp") < 16))

    return bars_df_transformed

