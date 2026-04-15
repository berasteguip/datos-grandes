from pyspark.sql import SparkSession
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

# ==========================================================
# CONFIGURACION
# ==========================================================
AWS_REGION = "eu-south-2"  # Madrid

BOOTSTRAP_SERVERS = "51.49.235.244:9092"
TOPIC_INPUT = "imat3a_ETH"
TOPIC_OUTPUT = "imat3a_ETH_VWAP"
GROUP_ID = "imat3a_group1_vwap_es"
CHECKPOINT_LOCATION = "/tmp/checkpoints/imat3a-eth-vwap-eu-south-2"

KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="kafka_client" password="88b8a35dca1a04da57dc5f3e";'
)

# ==========================================================
# SPARK
# ==========================================================
spark = (
    SparkSession.builder
    .appName("ETH_VWAP_Binance_Espana")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# ESQUEMA DE ENTRADA
# Soporta:
# 1) JSON simplificado:
#    {"symbol":"ETHUSDT","@timestamp":"2026-03-18T18:07:59Z","close":"1823.4","volume":"12.7"}
# 2) Mensaje raw de Binance kline:
#    {"e":"kline","E":...,"s":"ETHUSDT","k":{"t":...,"T":...,"s":"ETHUSDT","i":"1m","c":"1823.4","v":"12.7","x":true}}
# ==========================================================
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

# ==========================================================
# LEER DE KAFKA
# ==========================================================
df_kafka_input = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_INPUT)
    .option("startingOffsets", "latest")
    .option("kafka.group.id", GROUP_ID)
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

# Primero intentamos el timestamp ISO; si no existe, usamos el cierre de vela de Binance en milisegundos.
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

# ==========================================================
# VWAP 5 MIN / 1 MIN
# ==========================================================
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

query.awaitTermination()
