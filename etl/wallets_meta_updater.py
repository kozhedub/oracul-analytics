import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timezone
from rich.console import Console

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")


load_dotenv()
console = Console()

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

def update_wallets_meta():
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)

    # Загружаем транзакции
    df = pd.read_sql("token_transfers", engine)

    # Считаем first_tx, last_tx, tx_count, total_value для каждого address
    from_data = df.groupby("from_address").agg(
        first_tx_from=("timestamp", "min"),
        last_tx_from=("timestamp", "max"),
        tx_count_from=("tx_hash", "count"),
        total_value_from=("value_token", "sum")
    ).reset_index().rename(columns={"from_address": "address"})

    to_data = df.groupby("to_address").agg(
        first_tx_to=("timestamp", "min"),
        last_tx_to=("timestamp", "max"),
        tx_count_to=("tx_hash", "count"),
        total_value_to=("value_token", "sum")
    ).reset_index().rename(columns={"to_address": "address"})

    # Объединяем
    meta = pd.merge(from_data, to_data, on="address", how="outer").fillna(0)

    meta["first_tx_from"] = pd.to_datetime(meta["first_tx_from"], errors="coerce")
    meta["first_tx_to"] = pd.to_datetime(meta["first_tx_to"], errors="coerce")
    meta["last_tx_from"] = pd.to_datetime(meta["last_tx_from"], errors="coerce")
    meta["last_tx_to"] = pd.to_datetime(meta["last_tx_to"], errors="coerce")

    meta["first_tx"] = meta[["first_tx_from", "first_tx_to"]].min(axis=1)
    meta["last_tx"] = meta[["last_tx_from", "last_tx_to"]].max(axis=1)
    meta["tx_count"] = meta[["tx_count_from", "tx_count_to"]].sum(axis=1)
    meta["total_value"] = meta[["total_value_from", "total_value_to"]].sum(axis=1)

    meta = meta[["address", "first_tx", "last_tx", "tx_count", "total_value"]]

    # Запись в БД
    meta.to_sql("wallets_meta", engine, if_exists="replace", index=False)
    console.print("\n✅ [green]Таблица wallets_meta обновлена[/green]")

if __name__ == "__main__":
    update_wallets_meta()
