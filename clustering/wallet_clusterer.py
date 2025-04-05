import pandas as pd
import logging
from pathlib import Path
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

    # 📂 Путь к корневой папке проекта и data/
    BASE_DIR = Path(__file__).resolve().parent.parent
    data_dir = BASE_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_path = data_dir / "wallet_clusters.csv"
    df.to_csv(output_path, index=False)

    logging.info(f"✅ Кластеры записаны в wallet_clusters и сохранены в {output_path}")

if __name__ == "__main__":
    run_clustering()
