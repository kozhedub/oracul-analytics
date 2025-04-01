import os
from datetime import datetime, timezone
import requests
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

def update_token_prices():
    setup_logger()
    logging.info("📄 Запущен скрипт: prices.py")

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
            usd_value = prices.get(coingecko_id, {}).get("usd")
            if usd_value is not None:
                logging.info(f"🔍 Токен {symbol.upper()} → ${usd_value}")
                rows.append({
                    "token_symbol": symbol.upper(),
                    "usd_value": usd_value,
                    "updated_at": now
                })

        if rows:
            metadata = MetaData()
            metadata.reflect(bind=engine)

            if "token_prices" in metadata.tables:
                prices_table = metadata.tables["token_prices"]
                with engine.begin() as conn:
                    for row in rows:
                        stmt = insert(prices_table).values(**row)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["token_symbol"],
                            set_={
                                "usd_value": row["usd_value"],
                                "updated_at": row["updated_at"]
                            }
                        )
                        conn.execute(stmt)
                logging.info(f"✅ Загружено {len(rows)} курсов токенов в БД")

            else:
                logging.warning("⚠️ Таблица token_prices не найдена в БД.")

        else:
            logging.warning("⚠️ Нет данных для загрузки цен.")

    except Exception as e:
        logging.error(f"[Prices] Ошибка: {e}")
        print(f"❌ Ошибка при обновлении курсов: {e}")

if __name__ == "__main__":
    update_token_prices()
