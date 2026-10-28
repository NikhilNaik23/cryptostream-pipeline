from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)

# JS equivalent: a config object describing a scheduled job
with DAG(
    dag_id="daily_summary",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",          # every day at midnight UTC
    catchup=False,              # don't back-fill every missed day
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["cryptostream"],
) as dag:

    # {{ ds }} is Airflow's template for the run's date, like 2026-10-28
    build_daily_summary = SQLExecuteQueryOperator(
        task_id="build_daily_summary",
        conn_id="crypto_pg",
        sql="""
        INSERT INTO daily_summary
            (summary_date, symbol, open_price, close_price, high_price,
             low_price, vwap, total_volume, trade_count)
        SELECT
            '{{ ds }}'::date,
            symbol,
            (array_agg(price ORDER BY event_time ASC,  trade_id ASC))[1],
            (array_agg(price ORDER BY event_time DESC, trade_id DESC))[1],
            MAX(price),
            MIN(price),
            SUM(price * quantity) / NULLIF(SUM(quantity), 0),
            SUM(quantity),
            COUNT(*)
        FROM raw_trades
        WHERE event_time >= '{{ ds }}'::date
          AND event_time <  '{{ ds }}'::date + 1
        GROUP BY symbol
        ON CONFLICT (summary_date, symbol) DO UPDATE SET
            open_price   = EXCLUDED.open_price,
            close_price  = EXCLUDED.close_price,
            high_price   = EXCLUDED.high_price,
            low_price    = EXCLUDED.low_price,
            vwap         = EXCLUDED.vwap,
            total_volume = EXCLUDED.total_volume,
            trade_count  = EXCLUDED.trade_count
        """,
    )

    # Fails the run if no rows were written for that date
    check_rows_written = SQLCheckOperator(
        task_id="check_rows_written",
        conn_id="crypto_pg",
        sql="SELECT COUNT(*) FROM daily_summary WHERE summary_date = '{{ ds }}'::date",
    )

    build_daily_summary >> check_rows_written   # run the check after the build