## READ FROM FABRIC EVENTSTREAM
from pyspark.sql.functions import col, from_json, unbase64, when, decode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# EventHubs configuration
endpoint = "Endpoint=sb://.servicebus.windows.net/;SharedAccessKeyName=key_;SharedAccessKey=<>=;EntityPath=es_"
consumer_group = "cg_"

eh_conf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(endpoint),
    'eventhubs.consumerGroup': consumer_group,
    'eventhubs.startingPosition': "{\"offset\":\"-1\",\"seqNo\":-1,\"enqueuedTime\":null,\"isInclusive\":true}",
    'maxEventsPerTrigger': 500
}

# Define the schema based on the provided JSON structure
schema = StructType([
    StructField("Step", IntegerType(), True),
    StructField("Type", StringType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("NameOrig", StringType(), True),
    StructField("OldBalanceOrig", DoubleType(), True),
    StructField("NewBalanceOrig", DoubleType(), True),
    StructField("NameDest", StringType(), True),
    StructField("OldBalanceDest", DoubleType(), True),
    StructField("NewBalanceDest", DoubleType(), True),
    StructField("IsFraud", IntegerType(), True),
    StructField("IsFlaggedFraud", IntegerType(), True),
    StructField("Rec", IntegerType(), True),
    StructField("DateTimeNow", TimestampType(), True)
])

# Read stream, try to decode from base64 if possible, and parse as JSON
parsed_stream = spark.readStream.format("eventhubs").options(**eh_conf).load() \
    .select(
        when(col("body").cast("string").like("%=%"), 
             unbase64(col("body")).cast("string"))
        .otherwise(col("body").cast("string"))
        .alias("decoded_body")
    ) \
    .select(from_json(col("decoded_body"), schema).alias("data")) \
    .select("data.*")

# Write the stream to Parquet files
query = parsed_stream.coalesce(1).writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "Files/data") \
    .option("checkpointLocation", "Files/data/cp")\
    .trigger(processingTime='1 minute')  \
    .start()


query.awaitTermination()
