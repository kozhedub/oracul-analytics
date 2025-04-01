import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import logging

from utils.logger import setup_logger
setup_logger()

logging.info(f"📄 Запущен скрипт: {__file__}")



load_dotenv()

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

def run_wallet_clustering():
    print("🔗 Выполняем кластеризацию...")

    engine = create_engine(os.getenv("DATABASE_URL"))
    query = "SELECT address, first_tx, last_tx, tx_count, total_value FROM wallets_meta"
    df = pd.read_sql(query, engine)

    # Эвристическая кластеризация — можно улучшать
    df["cluster_id"] = (df["first_tx"].astype(str) + df["total_value"].astype(str)).factorize()[0]

    df.to_sql("wallet_clusters", engine, if_exists="replace", index=False)
    print("✅ Кластеры записаны в wallet_clusters")
