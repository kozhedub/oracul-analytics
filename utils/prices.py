import requests
import logging
import os
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone
from dotenv import load_dotenv

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


load_dotenv()

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

def fetch_prices_from_coingecko(symbols):
    """
    Получает цены токенов через CoinGecko.
    symbols — список тикеров (в нижнем регистре): ["usdc", "dai", "weth"]
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(symbols),
        "vs_currencies": "usd",
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Ошибка CoinGecko: {response.status_code} — {response.text}")
    return response.json()


def update_token_prices():
    logging.info("🚀 Запускаем обновление цен токенов")
    print("🚀 Обновляем курсы токенов...")

    # Примеры токенов — можно расширить или сделать динамическим
    token_map = {
        "usdc": "usd-coin",
        "dai": "dai",
        "weth": "weth",
        "usdt": "tether",
        "wbtc": "wrapped-bitcoin"
    }

    try:
        prices = fetch_prices_from_coingecko(list(token_map.values()))
    except Exception as e:
        logging.error(f"❌ Ошибка запроса CoinGecko: {str(e)}")
        print(f"❌ Ошибка CoinGecko: {str(e)}")
        return

    # Подключение к БД
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if "token_prices" not in metadata.tables:
        logging.warning("⛔ Таблица token_prices не существует")
        print("⛔ Таблица token_prices не найдена")
        return

    table = metadata.tables["token_prices"]
    now = datetime.utcnow()

    rows = []
    for symbol, coingecko_id in token_map.items():
        usd_value = prices.get(coingecko_id, {}).get("usd")
        if usd_value is not None:
            rows.append({
                "token_symbol": symbol.upper(),
                "usd_value": usd_value,
                "updated_at": now
            })

    with engine.begin() as conn:
        for row in rows:
            stmt = insert(table).values(**row)
            stmt = stmt.on_conflict_do_update(
                index_elements=["token_symbol"],
                set_={"usd_value": row["usd_value"], "updated_at": now}
            )
            conn.execute(stmt)

    logging.info(f"✅ Обновлены курсы {len(rows)} токенов")
    print(f"✅ Цены {len(rows)} токенов успешно обновлены")

