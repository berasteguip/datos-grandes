
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    struct,
    window,
    sum as spark_sum,
    round as spark_round,
    to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ==========================================================
# CONFIGURACION KAFKA
# ==========================================================
BOOTSTRAP_SERVERS = "51.49.235.244:9092"
TOPIC_INPUT = "imat3a_ETH"
TOPIC_OUTPUT = "imat3a_ETH_VWAP"
GROUP_ID = "imat3a_group1"

KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="kafka_client" password="88b8a35dca1a04da57dc5f3e";'
)

CHECKPOINT_LOCATION = "/tmp/checkpoints/imat3a-doge-vwap-final"
    
# ==========================================================
# 1. CREAR SESION SPARK
# ==========================================================
spark = (
    SparkSession.builder
    .appName("CryptoVWAPCalculator")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# 2. ESQUEMA DEL JSON DE ENTRADA
# Formato:
# {
#   "symbol": "DOGEUSDT",
#   "@timestamp": "2026-03-18T18:07:59Z",
#   "close": 0.09539,
#   "volume": 2328079.0
# }
# ==========================================================
input_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True)
])

# ==========================================================
# 3. LEER DATOS DEL TOPIC DE KAFKA DE ENTRADA
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

# ==========================================================
# 4. PARSEAR EL JSON DE ENTRADA
# ==========================================================
df_parsed = (
    df_kafka_input
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "timestamp AS kafka_timestamp"
    )
    .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        from_json(col("kafka_value"), input_schema).alias("data")
    )
    .select(
        col("data.symbol").alias("symbol"),
        col("data.close").cast("double").alias("price"),
        col("data.volume").cast("double").alias("volume"),
        col("data.`@timestamp`").alias("event_time")
    )
)

# ==========================================================
# 5. LIMPIEZA Y TRANSFORMACION
# ==========================================================
df_clean = (
    df_parsed
    .filter(col("symbol").isNotNull())
    .filter(col("price").isNotNull())
    .filter(col("volume").isNotNull())
    .filter(col("event_time").isNotNull())
    .filter(col("volume") > 0)
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX")
    )
    .filter(col("event_timestamp").isNotNull())
    .withColumn("price_x_volume", col("price") * col("volume"))
)

# ==========================================================
# 6. CALCULAR VWAP CADA MINUTO CON UNA VENTANA MOVIL
# DE LOS ULTIMOS 5 MINUTOS
# VWAP = sum(price * volume) / sum(volume)
#
# Usamos watermark de 0 segundos + output mode append para que
# Spark solo publique la ventana una vez cuando ya ha cerrado.
# Asi obtenemos 1 VWAP final por minuto, no 5 actualizaciones
# parciales del mismo conjunto de ventanas.
# ==========================================================
df_vwap = (
    df_clean
    .withWatermark("event_timestamp", "0 seconds")
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
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

# ==========================================================
# 7. FORMATEAR SALIDA PARA KAFKA
# key = simbolo
# value = JSON
#
# calculation_time = fin de la ventana
# Ejemplo:
#   window_start = 12:00
#   window_end   = 12:05
# => VWAP calculado con datos de los 5 minutos anteriores
# ==========================================================
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
                col("vwap").alias("vwap")
            )
        ).alias("value")
    )
)

# ==========================================================
# 8. ESCRIBIR RESULTADOS EN EL TOPIC DE SALIDA
# ==========================================================
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