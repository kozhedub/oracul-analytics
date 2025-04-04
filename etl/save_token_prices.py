import os
import logging
from datetime import datetime, timezone
from sqlalchemy import MetaData, text
from sqlalchemy.dialects.postgresql import insert
from utils.coingecko import get_token_prices
from utils.db_config import get_engine

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

# Получение движка для базы данных
engine = get_engine()

def save_prices_to_db(prices: dict):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables["token_prices"]

    with engine.begin() as conn:
        for symbol, price in prices.items():
            try:
                stmt = insert(table).values(
                    token_symbol=symbol,
                    price_usd=price,
                    updated_at=datetime.now(timezone.utc)
                ).on_conflict_do_update(
                    index_elements=["token_symbol"],
                    set_={
                        "price_usd": price,
                        "updated_at": datetime.now(timezone.utc)
                    }
                )
                conn.execute(stmt)
            except Exception as e:
                logging.error(f"❌ Ошибка записи в БД для {symbol}: {e}")

def fetch_and_save_token_prices():
    conn = engine.connect()

    # Берём токены из таблицы wallet_token_balances
    result = conn.execute(text("SELECT DISTINCT token_symbol FROM wallet_token_balances"))
    symbols = [row[0] for row in result]

    # Получаем цены из CoinGecko
    prices = get_token_prices(symbols=symbols)

    # Сохраняем полученные цены в базу данных
    save_prices_to_db(prices)

    logging.info(f"✅ Загружено {len(prices)} курсов токенов в БД")
    conn.close()

if __name__ == "__main__":
    fetch_and_save_token_prices()
