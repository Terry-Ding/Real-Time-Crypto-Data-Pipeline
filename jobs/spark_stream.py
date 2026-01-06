import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, max, min, first, last, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Standard Schema for Crypto Trades from Ingestion
def get_schema():
    return StructType([
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("timestamp", StringType(), False) # Sent as ISO string
    ])

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('CryptoDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'crypto_trades') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    return spark_df

def process_data(spark_df):
    schema = get_schema()

    # Parse JSON
    parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
    
    # Cast timestamp
    parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Windowed Aggregation (1 Minute)
    # OHLC: Open=first, High=max, Low=min, Close=last
    ohlc_df = parsed_df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(window(col("timestamp"), "1 minute"), col("symbol")) \
        .agg(
            first("price").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            last("price").alias("close"),
            sum("volume").alias("volume")
        ) \
        .select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            col("symbol"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume")
        )
        
    return ohlc_df

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        
        if spark_df is not None:
            final_df = process_data(spark_df)
            
            # Write to Parquet (Data Lake)
            # Using append mode with checkpointing
            query = final_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", "/opt/spark-data/data_lake/ohlc_data") \
                .option("checkpointLocation", "/opt/spark-data/data_lake/checkpoint") \
                .start()
            
            query.awaitTermination()