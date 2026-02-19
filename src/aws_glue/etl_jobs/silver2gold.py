from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)

INPUT_PATH = "s3://datos-grandes-eth-project/silver/"
OUTPUT_PATH = "s3://datos-grandes-eth-project/gold/"

ETH_SYMBOL = "BINANCE:ETHUSDT"

K_EMA50 = 300
K_EMA12 = 200
K_EMA26 = 300
K_EMA9  = 120

def ordered_history_array(value_col: str, order_col: str, w: Window) -> F.Column:
    return F.expr(
        f"transform(sort_array(collect_list(named_struct('k', {order_col}, 'v', {value_col})) over w), x -> x.v)"
    )

def ema_truncated_from_array(arr_col: str, span: int) -> F.Column:
    alpha = 2.0 / (span + 1.0)
    decay = 1.0 - alpha

    return F.expr(f"""
      CASE
        WHEN {arr_col} IS NULL OR size({arr_col}) = 0 THEN NULL
        ELSE
          aggregate(
            zip_with(
              {arr_col},
              sequence(0, size({arr_col}) - 1),
              (x, i) -> x * pow({decay}, (size({arr_col}) - 1 - i))
            ),
            CAST(0.0 AS DOUBLE),
            (acc, v) -> acc + v
          )
          /
          aggregate(
            sequence(0, size({arr_col}) - 1),
            CAST(0.0 AS DOUBLE),
            (acc, i) -> acc + pow({decay}, (size({arr_col}) - 1 - i))
          )
      END
    """)

spark = SparkSession.builder.appName("ETH_Indicators_SparkOnly_SMA_EMA_RSI_MACD").getOrCreate()

dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="trade_data_eth_imat3a03",
    table_name="eth_silver"
)

df = (
    dynamic_frame
    .toDF()
    .select(
        "datetime",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "year",
    )
)

df = df.withColumn("datetime", F.to_timestamp("datetime")).filter(F.col("datetime").isNotNull())

df = df.filter(F.col("symbol") == F.lit(ETH_SYMBOL))

w_order = Window.orderBy("datetime")

w200 = Window.orderBy("datetime").rowsBetween(-199, 0)
df = df.withColumn("SMA_200", F.avg("close").over(w200))

wK50 = Window.orderBy("datetime").rowsBetween(-(K_EMA50 - 1), 0)

close_hist_50 = (
    F.sort_array(F.collect_list(F.struct(F.col("datetime").alias("k"), F.col("close").alias("v"))).over(wK50))
)
df = df.withColumn("close_hist_50", F.transform(close_hist_50, lambda x: x["v"]))
df = df.withColumn("EMA_50", ema_truncated_from_array("close_hist_50", span=50)).drop("close_hist_50")

df = df.withColumn("prev_close", F.lag("close").over(w_order))
df = df.withColumn("delta", F.col("close") - F.col("prev_close"))

df = df.withColumn("gain", F.when(F.col("delta") > 0, F.col("delta")).otherwise(F.lit(0.0)))
df = df.withColumn("loss", F.when(F.col("delta") < 0, -F.col("delta")).otherwise(F.lit(0.0)))

w14 = Window.orderBy("datetime").rowsBetween(-13, 0)
df = df.withColumn("avg_gain_14", F.avg("gain").over(w14))
df = df.withColumn("avg_loss_14", F.avg("loss").over(w14))

df = df.withColumn(
    "RSI_14",
    F.when(F.col("avg_loss_14") == 0, F.lit(100.0))
     .otherwise(100.0 - (100.0 / (1.0 + (F.col("avg_gain_14") / F.col("avg_loss_14")))))
)

wK12 = Window.orderBy("datetime").rowsBetween(-(K_EMA12 - 1), 0)
close_hist_12 = F.sort_array(F.collect_list(F.struct(F.col("datetime").alias("k"), F.col("close").alias("v"))).over(wK12))
df = df.withColumn("close_hist_12", F.transform(close_hist_12, lambda x: x["v"]))
df = df.withColumn("EMA_12", ema_truncated_from_array("close_hist_12", span=12)).drop("close_hist_12")

wK26 = Window.orderBy("datetime").rowsBetween(-(K_EMA26 - 1), 0)
close_hist_26 = F.sort_array(F.collect_list(F.struct(F.col("datetime").alias("k"), F.col("close").alias("v"))).over(wK26))
df = df.withColumn("close_hist_26", F.transform(close_hist_26, lambda x: x["v"]))
df = df.withColumn("EMA_26", ema_truncated_from_array("close_hist_26", span=26)).drop("close_hist_26")

df = df.withColumn("MACD", F.col("EMA_12") - F.col("EMA_26"))

df = df.drop("prev_close", "delta", "gain", "loss", "avg_gain_14", "avg_loss_14", "EMA_12", "EMA_26")

df.write.mode("overwrite").partitionBy("year").parquet(OUTPUT_PATH)

spark.stop()
