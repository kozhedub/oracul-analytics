import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timezone
from dotenv import load_dotenv
from rich.console import Console
import requests

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")


console = Console()
load_dotenv()

# Получаем путь к корню проекта
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_PATH = os.path.join(BASE_DIR, "data", "balance_updater.log")

# Создаем папку, если нет
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# Конфиг логгера
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def send_telegram_message(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        console.print("⚠️ [yellow]TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не установлены в .env[/yellow]")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        logging.error(f"Ошибка Telegram: {e}")
        console.print(f"❌ [red]Ошибка при отправке в Telegram:[/red] {e}")

def update_wallet_balances(csv_path="addresses_with_balances.csv"):
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)

    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    csv_path = os.path.join(BASE_DIR, "data", "addresses_with_balances.csv")
    df_new = pd.read_csv(csv_path)

    try:
        df_existing = pd.read_sql("wallet_balances", con=engine)
    except Exception:
        df_existing = pd.DataFrame()

    if df_existing.empty:
        df_new["first_seen"] = datetime.utcnow()
        df_new["last_seen"] = datetime.utcnow()
        df_new["is_active"] = df_new["eth_balance"] > 0.1
        df_new.to_sql("wallet_balances", engine, if_exists="replace", index=False)
        msg = "✅ Создана новая таблица wallet_balances"
        console.print(f"{msg}")
        logging.info(msg)
        send_telegram_message(msg)
    else:
        updated_rows = []
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)

        for _, row in df_new.iterrows():
            addr = row["address"]
            match = df_existing[df_existing["address"] == addr]

            if match.empty:
                row["first_seen"] = now
                row["last_seen"] = now
                row["is_active"] = row["eth_balance"] > 0.1
            else:
                existing = match.iloc[0]
                row["first_seen"] = existing["first_seen"]
                row["last_seen"] = now
                row["is_active"] = row["eth_balance"] > 0.1

            updated_rows.append(row)

        df_final = pd.DataFrame(updated_rows)
        df_final.to_sql("wallet_balances", engine, if_exists="replace", index=False)
        msg = "✅ Обновлены балансы и метки активности в wallet_balances"
        console.print(f"{msg}")
        logging.info(msg)
        send_telegram_message(msg)

if __name__ == "__main__":
    update_wallet_balances()