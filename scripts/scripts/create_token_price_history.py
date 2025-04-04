# Скрипт для создания таблицы истории курсов токенов

from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

with engine.begin() as conn:
    conn.execute(text("""
CREATE TABLE IF NOT EXISTS token_price_history (
    id SERIAL PRIMARY KEY,
    token_symbol TEXT NOT NULL,
    price_usd NUMERIC,
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);
"""))
    print("✅ Таблица token_price_history создана (если не существовала)")
