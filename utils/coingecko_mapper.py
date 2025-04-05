import requests
import json
import logging
from utils.db_config import get_engine
from sqlalchemy import text
from pathlib import Path


logging.basicConfig(level=logging.INFO)

def fetch_symbols_from_db():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT token_symbol FROM wallet_token_balances"))

        return [row[0] for row in result.fetchall() if row[0] is not None]

def fetch_all_coingecko_tokens():
    url = "https://api.coingecko.com/api/v3/coins/list"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"❌ Ошибка при запросе к CoinGecko: {e}")
        return []

def build_symbol_to_id_map(symbols, coingecko_tokens):
    mapping = {}
    for symbol in symbols:
        matches = [t for t in coingecko_tokens if t["symbol"].lower() == symbol.lower()]
        if matches:
            # если несколько — берём первый, можно улучшить по full_name
            mapping[symbol] = matches[0]["id"]
        else:
            logging.warning(f"⚠️ Не найден CoinGecko ID для: {symbol}")
    return mapping

def save_to_json(data, filename="symbol_to_id_map.json"):
    BASE_DIR = Path(__file__).resolve().parent.parent
    output_path = BASE_DIR / "data" / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)  # создаёт папку, если её нет

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    logging.info(f"✅ Сохранено: {output_path}")

def main():
    symbols = fetch_symbols_from_db()
    logging.info(f"🔍 Найдено токенов в БД: {len(symbols)}")

    tokens = fetch_all_coingecko_tokens()
    logging.info(f"📦 Загружено {len(tokens)} токенов с CoinGecko")

    mapping = build_symbol_to_id_map(symbols, tokens)
    save_to_json(mapping)

if __name__ == "__main__":
    main()
