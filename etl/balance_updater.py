import os
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
from utils.logger import setup_logger
from utils.infura import get_eth_balance
from utils.etherscan import get_token_balances
import subprocess

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

# 🔧 Настройка
load_dotenv()

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

if not os.path.exists(csv_path):
    logging.error(f"❌ CSV-файл не найден: {csv_path}")
    print(f"❌ Файл {csv_path} не найден. Проверь путь и повтори попытку.")
    exit(1)

# 📥 Чтение адресов
addresses_df = pd.read_csv(csv_path)
if "address" not in addresses_df.columns:
    logging.error("❌ В CSV отсутствует колонка 'address'")
    exit(1)

# 💾 Подключение к БД
engine = create_engine(os.getenv("DATABASE_URL"))
metadata = MetaData()
metadata.reflect(bind=engine)

balances_data = []
token_snapshots = []

# 🔍 Получение балансов
for i, row in addresses_df.iterrows():
    address = row["address"].lower()
    label = row.get("label", None)

    try:
        balance_eth = get_eth_balance(address)
        tokens = get_token_balances(address)

        total_token_usd = 0
        for token in tokens:
            try:
                raw_balance = token.get("balance") or token.get("TokenQuantity") or token.get("value")
                if raw_balance is None:
                    continue
                decimals = int(token.get("tokenDecimal", 18))
                token_symbol = token.get("tokenSymbol", "UNKNOWN")
                contract = token.get("contractAddress") or token.get("tokenAddress")
                balance = float(raw_balance) / (10 ** decimals)

                token_snapshots.append({
                    "address": address,
                    "token_symbol": token_symbol,
                    "token_contract": contract,
                    "balance": balance,
                    "updated_at": datetime.now(timezone.utc)
                })

                total_token_usd += balance

            except Exception as token_err:
                logging.warning(f"[TOKEN ERROR] {address} {token.get('tokenSymbol')} — {token_err}")

        balances_data.append({
            "address": address,
            "balance_eth": balance_eth,
            "balance_token": total_token_usd,
            "usd_value": balance_eth + total_token_usd,
            "first_seen": datetime.now(timezone.utc),
            "last_seen": datetime.now(timezone.utc)
        })

        logging.info(f"[BAL] {label or address} ETH: {balance_eth:.4f}, Tokens: {total_token_usd:.2f} USD")

    except Exception as e:
        logging.warning(f"[BAL ERR] {label or address} — {e}")

if not balances_data:
    print("⚠️ Нет данных для записи в wallet_balances")
    exit(0)

# 📤 Загрузка в БД
with engine.begin() as conn:
    # 🧾 Основной баланс адреса
    if "wallet_balances" in metadata.tables:
        balances_table = metadata.tables["wallet_balances"]
        for row in balances_data:
            stmt = insert(balances_table).values(**row)
            stmt = stmt.on_conflict_do_update(
                index_elements=["address"],
                set_={
                    "balance_eth": row["balance_eth"],
                    "balance_token": row["balance_token"],
                    "usd_value": row["usd_value"],
                    "last_seen": row["last_seen"]
                }
            )
            conn.execute(stmt)

    # 🧾 Исторический snapshot по токенам
    if "wallet_token_balances" in metadata.tables:
        tokens_table = metadata.tables["wallet_token_balances"]
        for snap in token_snapshots:
            stmt = insert(tokens_table).values(**snap)
            stmt = stmt.on_conflict_do_update(
                index_elements=["address", "token_contract"],
                set_={
                    "balance": snap["balance"],
                    "updated_at": snap["updated_at"]
                }
            )
            conn.execute(stmt)

print(f"✅ Обновлено {len(balances_data)} адресов и {len(token_snapshots)} токенов")
logging.info(f"✅ Обновлено {len(balances_data)} балансов, сохранено {len(token_snapshots)} snapshot-записей")

# 🚀 Автоматический вызов wallet_token_tracker
try:
    subprocess.run(["python", os.path.join(BASE_DIR, "wallet_token_tracker.py")], check=True)
    logging.info("✅ Скрипт wallet_token_tracker.py выполнен успешно")
except Exception as e:
    logging.error(f"❌ Ошибка при запуске wallet_token_tracker.py: {e}")
