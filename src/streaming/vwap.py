from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as spark_sum, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
spark = SparkSession.builder \
    .appName("CryptoVWAPCalculator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()
BOOTSTRAP_SERVERS = "51.49.235.244:9092"
TOPIC_INPUT = "imat3a_ETH"
TOPIC_OUTPUT = "imat3a_ETH_VWAP"
CHECKPOINT_DIR = "s3://datos-grandes-eth-project/checkpoints/"

# Mecanismos de seguridad Kafka
KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="kafka_client" password="88b8a35dca1a04da57dc5f3e";'
)

kafka_options = {
    "kafka.bootstrap.servers": "51.49.235.244:9092",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka_client\" password=\"88b8a35dca1a04da57dc5f3e\";"
}

# 2. Leer datos del topic de Kafka de entrada
df_kafka_input = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .option("subscribe", TOPIC_INPUT) \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL) \
    .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM) \
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG) \
    .load()

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", StringType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

df_clean = df_kafka_input.selectExpr(
    "CAST(value AS STRING) AS json_value"
).select(
    from_json(col("json_value"), schema).alias("data")
).select(
    col("data.symbol").alias("symbol"),
    col("data.@timestamp").alias("event_time_str"),
    col("data.close").alias("price"),
    col("data.volume").alias("volume")
).withColumn(
    "event_time",
    to_timestamp(col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ssX")
)
df_vwap = df_clean \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        col("symbol"),
        window(col("event_time"), "5 minutes", "1 minute")
    ) \
    .agg(
        (spark_sum(col("price") * col("volume")) / spark_sum(col("volume"))).alias("vwap")
    )

df_output = df_vwap.select(
    col("symbol"),
    to_json(
        struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("vwap")
        )
    ).alias("value")
).selectExpr(
    "CAST(symbol AS STRING) AS key",
    "CAST(value AS STRING) AS value"
)
df_vwap.printSchema()
query = df_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("topic", TOPIC_OUTPUT) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .outputMode("update") \
    .start()

query.awaitTermination()

# job.commit()