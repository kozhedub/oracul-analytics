from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

with engine.begin() as conn:
    print("🛠️ Создаём таблицы истории...")

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS wallet_token_balances (
        id SERIAL PRIMARY KEY,
        address TEXT,
        token_symbol TEXT,
        balance NUMERIC,
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(address, token_symbol, updated_at)
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS wallet_eth_snapshots (
        id SERIAL PRIMARY KEY,
        address TEXT,
        balance_eth NUMERIC,
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(address, updated_at)
    );
    """))

print("✅ Таблицы истории созданы.")
