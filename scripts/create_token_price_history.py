import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, select, insert
from datetime import datetime, timezone
# Настройка
load_dotenv()
logging.basicConfig(level=logging.INFO)
logging.info(f"📄 Запущен скрипт: {__file__}")

# Подключение к БД
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)

# Таблицы
token_prices = metadata.tables.get("token_prices")
token_price_history = metadata.tables.get("token_price_history")

if not token_prices or not token_price_history:
    logging.error("❌ Таблицы 'token_prices' или 'token_price_history' не найдены в БД.")
    exit(1)

# Запрашиваем актуальные цены
with engine.connect() as conn:
    result = conn.execute(select(token_prices.c.token_symbol, token_prices.c.price_usd))
    rows = result.fetchall()

    if not rows:
        logging.warning("⚠️ Нет данных в token_prices.")
        exit(0)

    # Вставка истории


    now = datetime.now(timezone.utc)
    inserts = []
    for row in rows:
        if row["price_usd"] is not None:
            inserts.append({
                "token_symbol": row["token_symbol"],
                "price_usd": row["price_usd"],
                "fetched_at": now,
            })

    if inserts:
        conn.execute(insert(token_price_history), inserts)
        logging.info(f"✅ Добавлено {len(inserts)} записей в token_price_history.")
    else:
        logging.warning("⚠️ Нет данных для вставки.")
