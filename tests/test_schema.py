from processor.schema import TRADE_SCHEMA


def test_schema_has_expected_fields():
    assert TRADE_SCHEMA.fieldNames() == [
        "symbol", "trade_id", "price", "quantity", "event_time", "is_buyer_maker",
    ]