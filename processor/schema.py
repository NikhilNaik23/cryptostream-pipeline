from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRADE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("trade_id", LongType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("event_time", LongType()),   # milliseconds since 1970
    StructField("is_buyer_maker", BooleanType()),
])