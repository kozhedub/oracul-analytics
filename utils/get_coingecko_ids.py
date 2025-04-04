import json
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

def get_db_url():
    return os.getenv("DATABASE_URL")

def fetch_symbols_from_db():
    engine = create_engine(get_db_url())
    query = text("SELECT DISTINCT token_symbol FROM wallet_token_balances WHERE token_symbol IS NOT NULL")
    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result]

def fetch_coingecko_ids():
    url = "https://api.coingecko.com/api/v3/coins/list"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Ошибка при запросе к CoinGecko: {e}")
        return []

def map_symbols_to_ids(symbols, coingecko_list):
    symbol_id_map = {}
    unmatched = []

    symbols_set = set(s.lower() for s in symbols)
    for coin in coingecko_list:
        if coin["symbol"].lower() in symbols_set:
            symbol = coin["symbol"].upper()
            if symbol not in symbol_id_map:
                symbol_id_map[symbol] = coin["id"]

    for symbol in symbols:
        if symbol.upper() not in symbol_id_map:
            unmatched.append(symbol)

    return symbol_id_map, unmatched

if __name__ == "__main__":
    logging.info("📦 Загружаем токены из базы данных...")
    symbols = fetch_symbols_from_db()
    logging.info(f"🔢 Получено символов из БД: {len(symbols)}")

    logging.info("🌍 Загружаем список токенов с CoinGecko...")
    coins = fetch_coingecko_ids()
    logging.info(f"✅ Получено токенов с CoinGecko: {len(coins)}")

    logging.info("🔍 Сопоставляем символы...")
    mapping, not_found = map_symbols_to_ids(symbols, coins)

    with open("../symbol_to_id_map.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=4, ensure_ascii=False)

    logging.info(f"💾 Сохранено {len(mapping)} соответствий в symbol_to_id_map.json")

    if not_found:
        logging.warning(f"⚠️ Не найдены ID для следующих символов: {', '.join(not_found)}")
