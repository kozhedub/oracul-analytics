import os
import pandas as pd
from sqlalchemy import create_engine
import logging

from utils.logger import setup_logger
from utils.db_config import get_engine

setup_logger()
logging.info("🔗 Выполняем кластеризацию...")

def run_clustering():
    engine = get_engine()
    query = "SELECT DISTINCT address, wallet_label FROM wallet_token_balances"
    df = pd.read_sql(query, engine)

    df["cluster_id"] = df.groupby("wallet_label", dropna=False).ngroup()

    df.to_sql("wallet_clusters", engine, if_exists="replace", index=False)

    # 👉 Создаем папку data, если не существует
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/wallet_clusters.csv", index=False)

    logging.info("✅ Кластеры записаны в wallet_clusters")

if __name__ == "__main__":
    run_clustering()
