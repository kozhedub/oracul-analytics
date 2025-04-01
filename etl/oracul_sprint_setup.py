import os
import json
import pandas as pd
import logging
from tqdm import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from utils.etherscan import get_erc20_transactions
from utils.logger import setup_logger
from utils.telegram import send_telegram_message
from utils.prices import update_token_prices
from clustering.wallet_clusterer import run_wallet_clustering
import subprocess

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


setup_logger()
load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH")

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
        "0xa9059cbb": "ERC20 Transfer",○☼
        "0x23b872dd": "ERC721 Transfer",
        "0x6ea056a9": "Bridge",
        "0xf305d719": "DEX AddLiquidity",
        "0xe8e33700": "DEX RemoveLiquidity",
    }
    return signature_map.get(sig, "Contract Call")

logging.basicConfig(
    filename="processed.log",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

def insert_unique_rows(df, table_name, engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = insert(table).values(**row.to_dict())
            stmt = stmt.on_conflict_do_nothing(index_elements=["tx_hash"])
            conn.execute(stmt)

def collect_and_load(address, label=None, json_path="data/erc20_data.json", csv_path="data/erc20_data.csv"):
    import os
    import json
    import pandas as pd
    import logging
    from datetime import datetime
    from utils.etherscan import get_erc20_transactions
    from utils.telegram import send_telegram_message
    from sqlalchemy import create_engine, MetaData
    from sqlalchemy.dialects.postgresql import insert

    # 🛠 Убедимся, что папка "data/" существует
    os.makedirs("data", exist_ok=True)

    print(f"🔍 Получаем транзакции для {label or address}...")
    data = get_erc20_transactions(address)

    if not data or not data.get("result"):
        logging.warning(f"[{label or address}] Нет транзакций")
        print(f"⚠️  {label or address} — Нет данных, пропускаем")
        return

    # 💾 Сохраняем JSON-файл
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    # 📊 Преобразуем в DataFrame
    df = pd.DataFrame(data["result"])
    df["timestamp"] = pd.to_datetime(df["timeStamp"].astype(int), unit="s")
    df["value_token"] = df["value"].astype(float) / (10 ** df["tokenDecimal"].astype(int))
    df["token_contract"] = df["contractAddress"]
    df["token_symbol"] = df["tokenSymbol"]
    df["event_type"] = "Transfer"
    df["function_signature"] = df["input"].str[:10].str.lower()

    def detect_call_type(input_data: str) -> str:
        if not isinstance(input_data, str) or input_data == "0x":
            return "ETH Transfer"
        sig = input_data[:10].lower()
        signature_map = {
            "0x7ff36ab5": "DEX Swap",
            "0x38ed1739": "DEX Swap",
            "0xa9059cbb": "ERC20 Transfer",
            "0x23b872dd": "ERC721 Transfer",
            "0x6ea056a9": "Bridge",
            "0xf305d719": "DEX AddLiquidity",
            "0xe8e33700": "DEX RemoveLiquidity",
        }
        return signature_map.get(sig, "Contract Call")

    df["call_type"] = df["input"].apply(detect_call_type)

    df = df.rename(columns={
        "blockNumber": "block_number",
        "hash": "tx_hash",
        "from": "from_address",
        "to": "to_address"
    })

    df["wallet_label"] = label or None

    df = df[[
        "block_number", "tx_hash", "from_address", "to_address",
        "value_token", "timestamp", "wallet_label",
        "token_contract", "token_symbol", "event_type", "call_type"
    ]]

    # 💾 Сохраняем CSV
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logging.info(f"[{label or address}] Сохранено в {csv_path}")

    # 📤 Загружаем в PostgreSQL
    try:
        engine = create_engine(os.getenv("DATABASE_URL"))
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table = metadata.tables["token_transfers"]

        with engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = insert(table).values(**row.to_dict())
                stmt = stmt.on_conflict_do_nothing(index_elements=["tx_hash"])
                conn.execute(stmt)

        print(f"✅ {label or address} — Загружено {len(df)} записей в БД")
        logging.info(f"[{label or address}] Загружено: {len(df)} записей")
    except Exception as e:
        logging.error(f"[{label or address}] Ошибка загрузки в БД: {str(e)}")
        send_telegram_message(f"❌ Ошибка загрузки в БД: {label} — {str(e)}")


def collect_from_csv(csv_path=None):
    if csv_path is None:
        csv_path = os.getenv("ADDRESS_CSV_PATH")

    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV-файл не найден по пути: {csv_path}")

    addresses = pd.read_csv(csv_path)
    print(f"📋 Обработка {len(addresses)} адресов из {csv_path}")
    addresses = pd.read_csv(csv_path)
    print(f"📋 Обработка {len(addresses)} адресов из {csv_path}")

    for _, row in tqdm(addresses.iterrows(), total=len(addresses), desc="🧠 Oracul Scanning"):
        label = row.get("label")
        address = row["address"]
        try:
            collect_and_load(address, label)
        except Exception as e:
            logging.error(f"[{label}] Ошибка: {str(e)}")
            print(f"❌ [{label}] Ошибка: {e}")

    send_telegram_message("🧠 Oracul завершил анализ адресов!")

    subprocess.run(["python", os.path.join(BASE_DIR, "balance_updater.py")])
    subprocess.run(["python", os.path.join(BASE_DIR, "wallets_meta_updater.py")])

    try:
        update_token_prices()
        send_telegram_message("📈 Курсы токенов обновлены")
    except Exception as e:
        logging.error(f"[Prices] Ошибка: {str(e)}")
        send_telegram_message(f"❌ Ошибка при обновлении курсов: {str(e)}")

    try:
        run_wallet_clustering()
        send_telegram_message("🧹 Кластеризация завершена!")
    except Exception as e:
        logging.error(f"[Clustering] Ошибка: {str(e)}")
        send_telegram_message(f"❌ Ошибка при кластеризации: {str(e)}")

    try:
        subprocess.run(["python", "etl/whale_alerts.py"])
        send_telegram_message("📣 Whale Alerts завершены")
    except Exception as e:
        logging.error(f"[Whale Alerts] Ошибка: {str(e)}")
        send_telegram_message(f"❌ Ошибка при Whale Alerts: {str(e)}")

if __name__ == "__main__":
    collect_from_csv()
