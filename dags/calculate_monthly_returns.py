from common.db import PostgresDB
def calculate_monthly_return():
    db = PostgresDB()

    # Truy vấn giá close của ngày gần nhất hiện tại trong tháng
    latest_price_query = """
        SELECT date_trunc('day', to_timestamp(timestamp))::date AS day, close
        FROM ohlc_1d
        WHERE date_trunc('month', to_timestamp(timestamp)) = date_trunc('month', now())
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    latest = db.execute(latest_price_query)
    if not latest:
        raise ValueError("No latest price found for current month")

    # Truy vấn giá close của ngày cuối tháng trước
    prev_price_query = """
        SELECT date_trunc('day', to_timestamp(timestamp))::date AS day, close
        FROM ohlc_1d
        WHERE date_trunc('month', to_timestamp(timestamp)) = date_trunc('month', now() - interval '1 month')
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    previous = db.execute(prev_price_query)
    if not previous:
        raise ValueError("No closing price found for previous month")

    close_latest = latest[0]['close']
    close_prev = previous[0]['close']

    # Tính return %
    return_percent = ((close_latest - close_prev) / close_prev) * 100
    year = datetime.now().year
    month = datetime.now().month

    logging.info(f"Monthly return for {year}-{month}: {return_percent:.2f}%")

    upsert_query = """
        INSERT INTO btc_monthly_returns (year, month, return_percent)
        VALUES (:year, :month, :return_percent)
        ON CONFLICT (year, month)
        DO UPDATE SET return_percent = EXCLUDED.return_percent;
    """
    db.execute(upsert_query, {
        "year": year,
        "month": month,
        "return_percent": return_percent
    })