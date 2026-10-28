import json
import logging
import time

import websocket
from kafka import KafkaProducer

from producer.config import BINANCE_WS_URL, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("producer")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    linger_ms=50,
    retries=5,
)

sent_count = 0

def parse_trade(raw_message: str):
    payload = json.loads(raw_message)
    data = payload.get("data", {})
    
    if data.get("e") != "trade":
        return None
    
    return {
        "symbol": data["s"],
        "trade_id": data["t"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "event_time": data["T"],
        "is_buyer_maker": data["m"],
    }
    
def on_message(ws, message):
    global sent_count
    trade = parse_trade(message)
    if trade is None:
        return
    producer.send(KAFKA_TOPIC, key=trade["symbol"], value=trade)
    sent_count += 1
    
    if sent_count % 100 == 0:
        log.info("Sent %d trades | latest: %s %.2f", sent_count, trade["symbol"], trade["price"])
        
def on_error(ws, error):
    log.error("WebSocket error: %s", error)
    
def on_close(ws, close_status_code, close_msg):
    log.info("WebSocket closed (%s): %s", close_status_code, close_msg)
    
def on_open(ws):
    log.info("Connected to Binance. Streaming trades into topics %s", KAFKA_TOPIC)
    
def run():
    """Keep reconnecting if the connection drops."""
    while True:
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
        log.info("Reconnecting in 5 seconds...")
        time.sleep(5)
        
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Stopping producer...")
    finally:
        producer.flush()
        producer.close()