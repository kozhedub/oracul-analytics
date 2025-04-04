import os
from datetime import datetime, timezone
import requests
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dotenv import load_dotenv
import logging
from utils.logger import setup_logger


from sqlalchemy.sql import select

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

def update_token_prices():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    now = datetime.now(timezone.utc)

    token_map = {
        "USDC": "usd-coin",
        "WETH": "weth",
        "USDT": "tether",
        "DAI": "dai",
        "WBTC": "wrapped-bitcoin",
        "ELON": "dogelon-mars",
        "LINK": "chainlink",
        "MEME": "memecoin",
        "PEPE": "pepe"
    }

    url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(token_map.values())}&vs_currencies=usd"

    try:
        response = requests.get(url)
        response.raise_for_status()
        prices = response.json()

        rows = []
        for symbol, coingecko_id in token_map.items():
            price_usd = prices.get(coingecko_id, {}).get("usd")
            if price_usd is not None:
                logging.info(f"🔍 Токен {symbol.upper()} → ${price_usd}")
                rows.append({
                    "token_symbol": symbol.upper(),
                    "price_usd": price_usd,
                    "updated_at": now
                })

        if rows:
            metadata = MetaData()
            metadata.reflect(engine)
            if "token_prices" not in metadata.tables:
                logging.error("❌ Таблица 'token_prices' не найдена.")
                return

            prices_table = Table("token_prices", metadata, autoload_with=engine)

            with engine.begin() as conn:
                for row in rows:
                    stmt = pg_insert(prices_table).values(**row)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["token_symbol"],
                        set_={
                            "price_usd": row["price_usd"],
                            "updated_at": row["updated_at"]
                        }
                    )
                    conn.execute(stmt)

            logging.info(f"✅ Загружено {len(rows)} курсов токенов в БД")
        else:
            logging.warning("⚠️ Нет данных для загрузки цен.")

    except Exception as e:
        logging.error(f"[Prices] Ошибка: {e}")
        print(f"❌ Ошибка при обновлении курсов: {e}")

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import select

def save_prices_to_db(prices):
    """Сохраняет курсы токенов в таблицу token_prices, логируя старые значения перед обновлением."""
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table_obj = metadata.tables["token_prices"]

    df = pd.DataFrame.from_dict(prices, orient="index").reset_index()
    df.columns = ["token_symbol", "price_usd"]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            # Получаем старое значение
            old_value = conn.execute(
                select([table_obj.c.price_usd]).where(table_obj.c.token_symbol == row["token_symbol"])
            ).scalar()

            # Логируем, если курс изменился
            if old_value is not None and old_value != row["price_usd"]:
                logging.info(f"🔄 {row['token_symbol']}: {old_value} → {row['price_usd']}")

            # Вставляем или обновляем данные
            stmt = insert(table_obj).values(
                token_symbol=row["token_symbol"],
                price_usd=row["price_usd"]
            ).on_conflict_do_update(
                index_elements=["token_symbol"],
                set_={
                    "price_usd": row["price_usd"],
                    "updated_at": func.now()
                }
            )
            conn.execute(stmt)

    logging.info(f"✅ Загружено {len(df)} курсов токенов в БД")



if __name__ == "__main__":
    prices = fetch_token_prices()
    save_prices_to_db()
    print("✅ Курсы токенов обновлены в БД")
