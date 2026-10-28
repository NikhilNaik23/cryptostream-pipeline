import json

from producer.parser import parse_trade


def make_message(event_type="trade"):
    return json.dumps({
        "stream": "btcusdt@trade",
        "data": {"e": event_type, "s": "BTCUSDT", "t": 1,
                 "p": "80000.50", "q": "0.01", "T": 1761650000000, "m": False},
    })


def test_parse_trade_renames_and_converts():
    trade = parse_trade(make_message())
    assert trade["symbol"] == "BTCUSDT"
    assert trade["price"] == 80000.50      # string became float
    assert trade["quantity"] == 0.01
    assert trade["event_time"] == 1761650000000


def test_parse_trade_ignores_other_event_types():
    assert parse_trade(make_message("aggTrade")) is None