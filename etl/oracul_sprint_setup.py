import os
import json
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from utils.etherscan import get_erc20_transactions
from utils.telegram import send_telegram_message
from utils.logger import setup_logger
from utils.prices import update_token_prices
from clustering.wallet_clusterer import run_wallet_clustering
import logging
from datetime import datetime, timezone
from utils.prices import save_prices_to_db



setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

# Загрузка переменных окружения
load_dotenv()


# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

if not csv_path:
    raise ValueError("❌ ADDRESS_CSV_PATH не задан в .env")

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"❌ CSV-файл не найден по пути: {csv_path}")


def detect_call_type(input_data: str) -> str:
    if not isinstance(input_data, str) or input_data == "0x":
        return "ETH Transfer"
    sig = input_data[:10].lower()
    signature_map = {
        "0x7ff36ab5": "DEX Swap",
        "0x38ed1739": "DEX Swap",
        "0xa9059cbb": "ERC20 Transfer",
        "0x23b872dd": "ERC721 Transfer",
        "0xf305d719": "DEX AddLiquidity",
        "0xe8e33700": "DEX RemoveLiquidity",
    }
    return signature_map.get(sig, "Contract Call")


def insert_unique_rows(df, table_name, engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = insert(table).values(**row.to_dict())
            stmt = stmt.on_conflict_do_nothing(index_elements=["tx_hash"])
            conn.execute(stmt)


def collect_and_load(address, label=None, json_path="data/erc20_data.json", csv_path=None):
    os.makedirs("data", exist_ok=True)
    print(f"📥 Получаем транзакции для {label or address}...")
    data = get_erc20_transactions(address)

    if not data or not data.get("result"):
        logging.warning(f"⚠️ {label or address} — Нет данных, пропускаем")
        return

    # Сохраняем JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    # Преобразуем в DataFrame
    df = pd.DataFrame(data["result"])
    df["timestamp"] = pd.to_datetime(df["timeStamp"].astype(int), unit="s")
    df["value_token"] = df["value"].astype(float) / (10 ** df["tokenDecimal"].astype(int))
    df["token_contract"] = df["contractAddress"]
    df["token_symbol"] = df["tokenSymbol"]
    df["event_type"] = "Transfer"
    df["function_signature"] = df["input"].str[:10].str.lower()
    df["call_type"] = df["input"].apply(detect_call_type)

    df["wallet_label"] = label or ""
    df["to_address"] = df["to"]
    df["from_address"] = df["from"]

    logging.info(f"✅ Обработан: {label or address}")

    # Подключение к БД
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    insert_unique_rows(df, "token_transfers", engine)


def collect_from_csv(csv_path=None):
    df = pd.read_csv(csv_path or os.getenv("ADDRESS_CSV_PATH"))
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="📊 Обработка адресов"):
        address = row["address"]
        label = row.get("wallet_label") or f"TopHolder #{idx:03}"
        collect_and_load(address=address, label=label)


if __name__ == "__main__":
    logging.info("🚀 Старт сбора данных по токенам и транзакциям")
    update_token_prices()
    run_wallet_clustering()
    collect_from_csv(csv_path)
    logging.info("✅ Сбор данных завершён")
