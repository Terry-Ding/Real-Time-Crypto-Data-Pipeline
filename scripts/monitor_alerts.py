import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

def check_alerts():
    spark = SparkSession.builder \
        .appName('CryptoMonitor') \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Determine path relative to this script
        # Script is in <root>/scripts/
        # Data is in <root>/data_lake/
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, "data_lake", "ohlc_data")
        
        logging.info(f"Checking data at: {path}")
        print(f"Checking data at: {path}")
        
        try:
            df = spark.read.parquet(path)
        except Exception:
            logging.info("No data found yet in Data Lake.")
            return

        # Check for High Price Alert (e.g. BTC > 100000 - just an example threshold)
        # Or simple high volatility check.
        # Let's check max price in the dataset.
        
        max_price_row = df.agg(max("high").alias("max_price")).collect()[0]
        max_price = max_price_row["max_price"]
        
        if max_price and max_price > 100000:
            logging.warning(f"ALERT: Price Exceeded $100,000! Current Max: {max_price}")
            print(f"ALERT: Price Exceeded $100,000! Current Max: {max_price}")
        else:
            logging.info(f"Monitoring: Max price is {max_price}. No alerts.")
            print(f"Monitoring: Max price is {max_price}. No alerts.")

    except Exception as e:
        logging.error(f"Monitoring failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    check_alerts()
