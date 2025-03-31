import os
import logging
from sqlalchemy import create_engine, text
from utils.logger import setup_logger
from dotenv import load_dotenv

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


load_dotenv()
setup_logger()

logging.info("🛠️ Создание таблиц в базе данных...")
print("🛠️ Создаём таблицы...")

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS token_prices (
        token_symbol TEXT PRIMARY KEY,
        price_usd NUMERIC,
        updated_at TIMESTAMP DEFAULT NOW()
    )
    """))
    logging.info("✅ Таблица token_prices готова")

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS wallets_meta (
        address TEXT PRIMARY KEY,
        first_tx TIMESTAMP,
        last_tx TIMESTAMP,
        tx_count INTEGER,
        total_value NUMERIC,
        label TEXT
    )
    """))
    logging.info("✅ Таблица wallets_meta готова")

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS wallet_balances (
        address TEXT PRIMARY KEY,
        balance_eth NUMERIC,
        balance_token NUMERIC,
        usd_value NUMERIC,
        first_seen TIMESTAMP,
        last_seen TIMESTAMP
    )
    """))
    logging.info("✅ Таблица wallet_balances готова")

print("✅ Все таблицы успешно созданы!")
logging.info("🏁 Инициализация схемы завершена")
