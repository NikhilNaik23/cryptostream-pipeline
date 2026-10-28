import json


def parse_trade(raw_message: str):
    """Turn Binance's raw message into a clean, flat dictionary."""
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