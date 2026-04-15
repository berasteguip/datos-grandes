from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window as time_window, to_json, struct,
    sum as spark_sum, round as spark_round, current_timestamp, max as spark_max
)
from pyspark.sql.types import StructType, StructField, StringType

BOOTSTRAP_SERVERS = "51.49.235.244:9092"
TOPIC_INPUT = "imat3a_ETH"
TOPIC_OUTPUT = "imat3a_ETH_VWAP"
CHECKPOINT_LOCATION = "/tmp/checkpoints/eth-vwap-final-v3"

KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="kafka_client" password="88b8a35dca1a04da57dc5f3e";'
)

spark = (
    SparkSession.builder
    .appName("ETH_VWAP_1Min_Arrivals")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True)
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

df_clean = (
    df_kafka_input
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), input_schema).alias("data"))
    .select(
        col("data.symbol").alias("symbol"),
        col("data.close").cast("double").alias("price"),
        col("data.volume").cast("double").alias("volume"),
        to_timestamp(
            col("data.`@timestamp`"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"
        ).alias("event_timestamp")
    )
    .filter(
        col("symbol").isNotNull() &
        col("price").isNotNull() &
        col("volume").isNotNull() &
        col("event_timestamp").isNotNull() &
        (col("volume") > 0)
    )
    .withColumn("price_x_volume", col("price") * col("volume"))
)

df_vwap = (
    df_clean
    .groupBy(
        time_window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("symbol")
    )
    .agg(
        spark_sum("price_x_volume").alias("weighted_price_sum"),
        spark_sum("volume").alias("volume_sum")
    )
    .filter(col("volume_sum") > 0)
    .withColumn(
        "vwap",
        spark_round(col("weighted_price_sum") / col("volume_sum"), 8)
    )
)

def publish_exact_window(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    valid_windows = batch_df.filter(col("window.end") <= current_timestamp())

    if valid_windows.rdd.isEmpty():
        return

    latest_end_df = (
        valid_windows
        .groupBy("symbol")
        .agg(spark_max(col("window.end")).alias("latest_window_end"))
    )

    final_df = (
        valid_windows
        .join(
            latest_end_df,
            on=[
                valid_windows["symbol"] == latest_end_df["symbol"],
                valid_windows["window.end"] == latest_end_df["latest_window_end"]
            ],
            how="inner"
        )
        .select(
            valid_windows["symbol"].cast("string").alias("key"),
            to_json(
                struct(
                    valid_windows["window.start"].alias("window_start"),
                    valid_windows["window.end"].alias("window_end"),
                    valid_windows["symbol"].alias("symbol"),
                    valid_windows["vwap"].alias("vwap")
                )
            ).alias("value")
        )
    )

    final_df.show(truncate=False)

    (
        final_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_OUTPUT)
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
        .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
        .save()
    )

query = (
    df_vwap.writeStream
    .outputMode("update")
    .foreachBatch(publish_exact_window)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="1 minute")
    .start()
)

query.awaitTermination()

