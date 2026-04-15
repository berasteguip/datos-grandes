import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    coalesce,
    col,
    from_json,
    from_unixtime,
    round as spark_round,
    struct,
    sum as spark_sum,
    to_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType


required_args = [
    "JOB_NAME",
    "BOOTSTRAP_SERVERS",
    "TOPIC_INPUT",
    "TOPIC_OUTPUT",
    "CHECKPOINT_LOCATION",
    "KAFKA_USERNAME",
    "KAFKA_PASSWORD",
]
args = getResolvedOptions(sys.argv, required_args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("WARN")

BOOTSTRAP_SERVERS = args["BOOTSTRAP_SERVERS"]
TOPIC_INPUT = args["TOPIC_INPUT"]
TOPIC_OUTPUT = args["TOPIC_OUTPUT"]
CHECKPOINT_LOCATION = args["CHECKPOINT_LOCATION"]
KAFKA_USERNAME = args["KAFKA_USERNAME"]
KAFKA_PASSWORD = args["KAFKA_PASSWORD"]

KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

# Soporta:
# 1) JSON simplificado:
#    {"symbol":"ETHUSDT","@timestamp":"2026-03-18T18:07:59Z","close":"1823.4","volume":"12.7"}
# 2) Mensaje raw de Binance kline:
#    {"e":"kline","E":...,"s":"ETHUSDT","k":{"t":...,"T":...,"s":"ETHUSDT","i":"1m","c":"1823.4","v":"12.7","x":true}}
binance_kline_schema = StructType([
    StructField("T", LongType(), True),
    StructField("s", StringType(), True),
    StructField("i", StringType(), True),
    StructField("c", StringType(), True),
    StructField("v", StringType(), True),
    StructField("x", StringType(), True),
])

input_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("s", StringType(), True),
    StructField("k", binance_kline_schema, True),
])

df_kafka_input = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_INPUT)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
    .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
    .load()
)

df_parsed = (
    df_kafka_input
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "timestamp AS kafka_timestamp",
    )
    .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        from_json(col("kafka_value"), input_schema).alias("data"),
    )
)

df_clean = (
    df_parsed
    .select(
        coalesce(
            col("data.symbol"),
            col("data.k.s"),
            col("data.s"),
            col("kafka_key"),
        ).alias("symbol"),
        coalesce(
            col("data.close").cast("double"),
            col("data.k.c").cast("double"),
        ).alias("price"),
        coalesce(
            col("data.volume").cast("double"),
            col("data.k.v").cast("double"),
        ).alias("volume"),
        coalesce(
            to_timestamp(col("data.`@timestamp`"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
            to_timestamp(col("data.`@timestamp`"), "yyyy-MM-dd'T'HH:mm:ssX"),
            to_timestamp(from_unixtime(col("data.k.T") / 1000.0)),
            col("kafka_timestamp"),
        ).alias("event_timestamp"),
    )
    .filter(col("symbol").isNotNull())
    .filter(col("price").isNotNull())
    .filter(col("volume").isNotNull())
    .filter(col("event_timestamp").isNotNull())
    .filter(col("volume") > 0)
    .withColumn("price_x_volume", col("price") * col("volume"))
)

df_vwap = (
    df_clean
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("symbol"),
    )
    .agg(
        spark_sum("price_x_volume").alias("weighted_price_sum"),
        spark_sum("volume").alias("volume_sum"),
    )
    .filter(col("volume_sum") > 0)
    .withColumn(
        "vwap",
        spark_round(col("weighted_price_sum") / col("volume_sum"), 8),
    )
)

df_kafka_output = (
    df_vwap
    .select(
        col("symbol").cast("string").alias("key"),
        to_json(
            struct(
                col("window.end").alias("calculation_time"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("symbol").alias("symbol"),
                col("vwap").alias("vwap"),
                col("volume_sum").alias("volume_sum"),
            )
        ).alias("value"),
    )
)

query = (
    df_kafka_output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", TOPIC_OUTPUT)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
    .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
    .outputMode("append")
    .start()
)

try:
    query.awaitTermination()
finally:
    job.commit()
