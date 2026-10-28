import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")

SYMBOLS = [
    s.strip().lower()
    for s in os.getenv("SYMBOLS", "btcusdt").split(",")
    if s.strip()
]

BINANCE_WS_URL = "wss://data-stream.binance.vision/stream?streams=" + "/".join(
    f"{symbol}@trade" for symbol in SYMBOLS
)