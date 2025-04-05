import requests
import logging
import os
import json
from datetime import datetime, timedelta

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

COINGECKO_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"
COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
CACHE_FILE = os.path.join(BASE_DIR, "../data/coingecko_tokens.json")

CACHE_TTL_HOURS = 24

def load_cached_ids() -> dict:
    if os.path.exists(CACHE_FILE):
        cache_time = datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
        if datetime.now() - cache_time < timedelta(hours=CACHE_TTL_HOURS):
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    return {}

def save_cached_ids(data: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def fetch_all_coins() -> dict:
    logging.info("🌍 Получаем список токенов с CoinGecko")
    try:
        response = requests.get(COINGECKO_LIST_URL)
        response.raise_for_status()
        coins = response.json()
        return {coin['symbol'].upper(): coin['id'] for coin in coins}
    except Exception as e:
        logging.error(f"❌ Ошибка при получении списка токенов: {e}")
        return {}

def get_coingecko_ids(symbols: list) -> dict:
    cached_ids = load_cached_ids()
    if not cached_ids:
        cached_ids = fetch_all_coins()
        save_cached_ids(cached_ids)

    result = {}
    for symbol in symbols:
        if symbol.upper() in cached_ids:
            result[symbol.upper()] = cached_ids[symbol.upper()]
        else:
            logging.warning(f"⚠️ Не найден CoinGecko ID для: {symbol}")
    return result

def get_token_prices(symbols: list) -> dict:
    """
    Возвращает цены токенов в USD: {"USDT": 1.0, "LINK": 13.2, ...}
    """
    logging.info(f"🌍 Получаем курсы для токенов: {symbols}")
    id_map = get_coingecko_ids(symbols)

    if not id_map:
        logging.warning("⚠️ Нет ID токенов для запроса")
        return {}

    ids_str = ",".join(set(id_map.values()))
    params = {"ids": ids_str, "vs_currencies": "usd"}

    try:
        response = requests.get(COINGECKO_PRICE_URL, params=params)
        response.raise_for_status()
        prices = response.json()

        # Вернуть обратно к символам
        reverse_map = {v: k for k, v in id_map.items()}
        result = {}
        for coin_id, data in prices.items():
            symbol = reverse_map.get(coin_id)
            if symbol and "usd" in data:
                result[symbol] = data["usd"]
        return result

    except Exception as e:
        logging.error(f"❌ Ошибка получения цен с CoinGecko: {e}")
        return {}
