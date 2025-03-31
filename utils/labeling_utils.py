import pandas as pd
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


load_dotenv()


def update_wallet_labels():
    try:
        db_url = os.getenv("DATABASE_URL")
        engine = create_engine(db_url)

        # Загружаем текущие балансы
        df = pd.read_sql("SELECT * FROM wallet_balances", engine)

        # Простая логика для назначения меток
        df["label"] = ""
        df.loc[df["balance_token"] > 100000, "label"] = "Whale"
        df.loc[df["balance_token"] > 1000000, "label"] = "Super Whale"
        df.loc[df["wallet_label"].str.contains("DAO", na=False), "label"] = "DAO"
        df.loc[df["wallet_label"].str.contains("Binance|Coinbase|OKX|Kraken", na=False), "label"] = "CEX"

        # Обновляем или создаём таблицу
        df_out = df[["wallet_address", "label"]].drop_duplicates()
        df_out.to_sql("wallet_labels", engine, if_exists="replace", index=False)

        logging.info("[Labeling] Метки адресов успешно обновлены")
    except Exception as e:
        logging.error(f"[Labeling] Ошибка: {e}")
        raise
