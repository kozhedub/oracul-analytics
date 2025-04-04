import os
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
from utils.etherscan import get_token_balances

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

load_dotenv()

logging.info("🚀 Запуск отслеживания токенов на адресах")
print("🚀 Сканируем токены по адресам...")

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

if not os.path.exists(csv_path):
    logging.error(f"❌ CSV-файл не найден: {csv_path}")
    print(f"❌ Файл {csv_path} не найден. Проверь путь и повтори попытку.")
    exit(1)

addresses = pd.read_csv(csv_path)
rows = []

for _, row in addresses.iterrows():
    address = row["address"]
    label = row.get("label", None)

    try:
        tokens = get_token_balances(address)

        for token in tokens:
            raw_balance = token.get("balance") or token.get("TokenQuantity") or token.get("value")
            if raw_balance is None:
                continue

            decimals = int(token.get("tokenDecimal", 18))
            balance = float(raw_balance) / (10 ** decimals)

            rows.append({
                "address": address,
                "token_symbol": token.get("tokenSymbol"),
                "token_contract": token.get("contractAddress"),
                "balance": balance,
                "updated_at": datetime.now(timezone.utc)
            })

        logging.info(f"[TOKENS] {label or address} — {len(tokens)} токенов")

    except Exception as e:
        logging.warning(f"[TOKENS ERR] {label or address} — {str(e)}")

if not rows:
    print("⚠️ Нет токенов для записи")
    exit(0)

# Запись в БД
engine = create_engine(os.getenv("DATABASE_URL"))
metadata = MetaData()
metadata.reflect(bind=engine)
table = metadata.tables["wallet_token_balances"]

with engine.begin() as conn:
    for row in rows:
        stmt = insert(table).values(**row)
        stmt = stmt.on_conflict_do_update(
            index_elements=["address", "token_contract"],
            set_={
                "balance": row["balance"],
                "updated_at": row["updated_at"]
            }
        )
        conn.execute(stmt)

print(f"✅ Записано {len(rows)} токенов в wallet_token_balances")
logging.info(f"✅ Записано {len(rows)} токенов в БД")


def scan_tokens_for_all():
    """Сканирует токены по всем адресам из CSV и обновляет таблицу wallet_token_balances"""
    logging.info("🚀 Запуск отслеживания токенов на адресах")
    print("🚀 Сканируем токены по адресам...")

    # 📂 Пути
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

    if not os.path.exists(csv_path):
        logging.error(f"❌ CSV-файл не найден: {csv_path}")
        print(f"❌ Файл {csv_path} не найден. Проверь путь и повтори попытку.")
        return

    addresses = pd.read_csv(csv_path)
    rows = []

    for _, row in addresses.iterrows():
        address = row["address"]
        label = row.get("label", None)

        try:
            tokens = get_token_balances(address)

            for token in tokens:
                raw_balance = token.get("balance") or token.get("TokenQuantity") or token.get("value")
                if raw_balance is None:
                    continue

                decimals = int(token.get("tokenDecimal", 18))
                balance = float(raw_balance) / (10 ** decimals)

                rows.append({
                    "address": address,
                    "token_symbol": token.get("tokenSymbol"),
                    "token_contract": token.get("contractAddress"),
                    "balance": balance,
                    "updated_at": datetime.now(timezone.utc)
                })

            logging.info(f"[TOKENS] {label or address} — {len(tokens)} токенов")

        except Exception as e:
            logging.warning(f"[TOKENS ERR] {label or address} — {str(e)}")

    if not rows:
        print("⚠️ Нет токенов для записи")
        return

    # Запись в БД
    engine = create_engine(os.getenv("DATABASE_URL"))
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables["wallet_token_balances"]

    with engine.begin() as conn:
        for row in rows:
            stmt = insert(table).values(**row)
            stmt = stmt.on_conflict_do_update(
                index_elements=["address", "token_contract"],
                set_={
                    "balance": row["balance"],
                    "updated_at": row["updated_at"]
                }
            )
            conn.execute(stmt)

    logging.info(f"✅ Записано {len(rows)} токенов в БД")
    print(f"✅ Записано {len(rows)} токенов в wallet_token_balances")

def scan_tokens_for_wallet(address: str, label: str = None) -> pd.DataFrame:
    """
    Получает токены по адресу через Etherscan, возвращает DataFrame.
    """
    from utils.etherscan import get_erc20_transactions

    result = get_erc20_transactions(address)
    if result["status"] == "0":
        return pd.DataFrame()

    df = pd.DataFrame(result["result"])

    if not df.empty and label:
        df["wallet_label"] = label

    return df
