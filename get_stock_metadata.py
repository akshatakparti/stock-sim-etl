import yfinance as yf
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import lit,col,from_unixtime


# --- Config ---
schema_path = "dim_stock_schema.json"

# --- Initialize Spark ---
spark = SparkSession.builder.appName("IngestStockMetadata").getOrCreate()

# --- Process each ticker ---

def getStockMetadata(ticker_name:str):

    ticker = yf.Ticker(ticker_name)

    # data returned as a flat dictionary
    response_dict = ticker.info

    # convert to json string -> rdd -> df
    response_json = json.dumps(response_dict)
    response_rdd = spark.sparkContext.parallelize([response_json])
    df = spark.read.json(response_rdd)

    # column mapping 
    column_mapping = {
        "shortName":"stockName",
        "fullExchangeName":"exchangeName",
        "firstTradeDateMilliseconds":"ipoDate"
    }

    # rename columns based on mapping
    for old_col,new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col,new_col)

    # load schema from external JSON file and parse using StructType
    from pyspark.sql.types import StructType

    with open(schema_path, 'r') as file:
        schema_json = json.load(file)

    schema = StructType.fromJson(schema_json)

    # get data for columns matching schema
    columns_to_keep = [field.name for field in schema.fields]

    # dividendYield may not exist for all stocks - add a default value if field is missing
    for column in columns_to_keep:
        if column not in df.columns:
            df = df.withColumn("dividendYield",lit(0))

    df_cleaned = df.select(*columns_to_keep)

    # convert ipoDate from milliseconds to date
    from datetime import datetime

    df_cleaned = df_cleaned.withColumn("ipoDate",from_unixtime(col("ipoDate") / 1000).cast("date"))

    return df_cleaned