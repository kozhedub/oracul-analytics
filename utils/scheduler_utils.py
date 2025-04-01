def get_recently_active(engine, days=30):
    from sqlalchemy.sql import text
    import datetime

    print("📄 Запущен скрипт: <название>")
    logging.info("📄 Запущен скрипт: <название>")

    query = text("""
        SELECT wallet_label, MAX(timestamp) as last_tx
        FROM token_transfers
        WHERE wallet_label IS NOT NULL
        GROUP BY wallet_label
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    return {row[0]: row[1] for row in result if row[1] >= cutoff}
