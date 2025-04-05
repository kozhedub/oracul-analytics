import logging

from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
csv_path = Path(os.getenv("ADDRESS_CSV_PATH", BASE_DIR / "data" / "addresses.csv"))
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, insert, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils.logger import setup_logger
from utils.db_config import get_engine
from utils.telegram import send_telegram_message
from utils.etherscan import get_erc20_transactions
from clustering.wallet_clusterer import run_clustering
from etl.wallet_token_tracker import scan_tokens_for_wallet
from etl.balance_updater import update_wallet_balances
from etl.save_token_prices import fetch_and_save_token_prices

# 📂 Пути


# Настройка окружения
load_dotenv()
setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

engine = get_engine()

# Уникальная вставка записей в таблицу
def insert_unique_rows(df, table_name, engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table_obj = metadata.tables.get(table_name)

    if not table_obj:
        logging.error(f"❌ Таблица {table_name} не найдена в базе данных.")
        return

    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = pg_insert(table_obj).values(row.to_dict())
            stmt = stmt.on_conflict_do_nothing(index_elements=["tx_hash"])
            try:
                conn.execute(stmt)
            except Exception as e:
                logging.error(f"❌ Ошибка вставки строки: {e}")

# Обработка одного адреса
def collect_and_load(address, label=None):
    logging.info(f"📥 Получаем транзакции для {address} ({label})")
    df = get_erc20_transactions(address)
    if not isinstance(df, pd.DataFrame) or df.empty:
        logging.warning(f"⚠️ Нет транзакций для {address}")
        return
    df["wallet_label"] = label
    insert_unique_rows(df, "token_transfers", engine)

# Обработка из CSV
def collect_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    tqdm.pandas(desc="📊 Обработка адресов")
    df.progress_apply(lambda row: collect_and_load(row["address"], row.get("wallet_label")), axis=1)

# Запуск основного пайплайна
try:
    logging.info("🚀 Старт сбора данных по токенам и транзакциям")
    fetch_and_save_token_prices()
    run_clustering()
    csv_path = os.getenv("ADDRESS_CSV", "data/addresses.csv")
    collect_from_csv(csv_path)
    update_wallet_balances()
    logging.info("✅ Обновление балансов завершено")
    logging.info("🏁 Все данные успешно обновлены ✅")
    send_telegram_message("✅ Oracul: данные успешно обновлены")

except Exception as e:
    logging.error(f"❌ Ошибка в основном пайплайне: {e}")
    send_telegram_message(f"❌ Ошибка в Oracul: {e}")
