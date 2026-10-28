import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processor.schema import TRADE_SCHEMA

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")
ALERT_THRESHOLD_PCT = float(os.getenv("ALERT_THRESHOLD_PCT", "0.5"))

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "cryptostream"),
    "user": os.getenv("POSTGRES_USER", "crypto"),
    "password": os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
}


# ---------- 1. Start Spark ----------
spark = (
    SparkSession.builder.appName("CryptoStream")
    .master("local[*]")  # run on all CPU cores of your machine
    # Downloads the Kafka connector the first time (needs internet)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")  # default 200 is overkill locally
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ---------- 2. Read from Kafka ----------
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ---------- 3. Parse JSON into columns ----------
# JS: messages.map(m => JSON.parse(m.value)).filter(t => t.price != null)
trades = (
    kafka_df.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
    .select("t.*")
    .dropna(subset=["symbol", "price", "quantity", "event_time"])
    .withColumn("event_ts", (F.col("event_time") / 1000).cast("timestamp"))
)

# ---------- 4. Calculate stats per 1-minute window per coin ----------
metrics = (
    trades.withWatermark("event_ts", "10 seconds")  # wait up to 10s for late trades
    .groupBy(F.window("event_ts", "1 minute"), "symbol")
    .agg(
        F.avg("price").alias("avg_price"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.stddev("price").alias("volatility"),
        F.count("*").alias("trade_count"),
        F.sum("quantity").alias("total_volume"),
    )
    .select(
        "symbol",
        F.col("window.start").cast("long").alias("window_start"),  # epoch seconds
        F.col("window.end").cast("long").alias("window_end"),
        "avg_price", "vwap", "min_price", "max_price",
        "volatility", "trade_count", "total_volume",
    )
)


# ---------- 5. Write to Postgres ----------
def to_dt(epoch_seconds):
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


def run_sql(sql, rows):
    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn, conn.cursor() as cur:  # "with conn" commits on success
            execute_values(cur, sql, rows)
    finally:
        conn.close()


def write_raw(batch_df, batch_id):
    """Spark calls this every 5 seconds with the new trades (a mini table)."""
    rows = [
        (r.symbol, r.trade_id, r.price, r.quantity, to_dt(r.event_time / 1000), r.is_buyer_maker)
        for r in batch_df.collect()  # collect() = pull rows into a normal Python list
    ]
    if not rows:
        return
    run_sql(
        """
        INSERT INTO raw_trades (symbol, trade_id, price, quantity, event_time, is_buyer_maker)
        VALUES %s
        ON CONFLICT (symbol, trade_id) DO NOTHING
        """,
        rows,
    )


def write_metrics(batch_df, batch_id):
    """Insert the window row, or update it if it already exists (upsert)."""
    rows = [
        (r.symbol, to_dt(r.window_start), to_dt(r.window_end), r.avg_price, r.vwap,
         r.min_price, r.max_price, r.volatility, r.trade_count, r.total_volume)
        for r in batch_df.collect()
    ]
    if not rows:
        return
    run_sql(
        """
        INSERT INTO trade_metrics
            (symbol, window_start, window_end, avg_price, vwap, min_price,
             max_price, volatility, trade_count, total_volume)
        VALUES %s
        ON CONFLICT (symbol, window_start, window_end) DO UPDATE SET
            avg_price = EXCLUDED.avg_price,
            vwap = EXCLUDED.vwap,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            volatility = EXCLUDED.volatility,
            trade_count = EXCLUDED.trade_count,
            total_volume = EXCLUDED.total_volume
        """,
        rows,
    )

# ---------- 5b. Price-spike detection ----------
# first/last price per 30-second window per coin, then the % move between them.
movement = (
    trades.withWatermark("event_ts", "10 seconds")
    .groupBy(F.window("event_ts", "30 seconds"), "symbol")
    .agg(
        F.min_by("price", "event_ts").alias("first_price"),  # price at the earliest trade
        F.max_by("price", "event_ts").alias("last_price"),   # price at the latest trade
    )
    .select(
        "symbol",
        F.col("window.start").cast("long").alias("window_start"),
        F.col("window.end").cast("long").alias("window_end"),
        "first_price",
        "last_price",
        ((F.col("last_price") - F.col("first_price")) / F.col("first_price") * 100).alias("move_pct"),
    )
)


def write_alerts(batch_df, batch_id):
    """Keep only windows that moved at least the threshold, then upsert them."""
    # JS: rows.filter(r => Math.abs(r.move_pct) >= THRESHOLD).map(r => [...])
    rows = [
        (r.symbol, to_dt(r.window_start), to_dt(r.window_end),
         r.first_price, r.last_price, r.move_pct,
         "UP" if r.move_pct > 0 else "DOWN")
        for r in batch_df.collect()
        if r.move_pct is not None and abs(r.move_pct) >= ALERT_THRESHOLD_PCT
    ]
    if not rows:
        return
    run_sql(
        """
        INSERT INTO price_alerts
            (symbol, window_start, window_end, first_price, last_price, move_pct, direction)
        VALUES %s
        ON CONFLICT (symbol, window_start, window_end) DO UPDATE SET
            first_price = EXCLUDED.first_price,
            last_price = EXCLUDED.last_price,
            move_pct = EXCLUDED.move_pct,
            direction = EXCLUDED.direction
        """,
        rows,
    )

# ---------- 6. Start both streams ----------
raw_query = (
    trades.writeStream.foreachBatch(write_raw)
    .option("checkpointLocation", "checkpoints/raw")
    .trigger(processingTime="5 seconds")
    .start()
)

metrics_query = (
    metrics.writeStream.outputMode("update")  # only send windows that changed
    .foreachBatch(write_metrics)
    .option("checkpointLocation", "checkpoints/metrics")
    .trigger(processingTime="5 seconds")
    .start()
)

alerts_query = (
    movement.writeStream.outputMode("update")
    .foreachBatch(write_alerts)
    .option("checkpointLocation", "checkpoints/alerts")
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark streaming started. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()