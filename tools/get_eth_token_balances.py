import os
import time
import pandas as pd
import requests
from dotenv import load_dotenv
from web3 import Web3
from tqdm import tqdm
from sqlalchemy import create_engine

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")


load_dotenv()

INFURA_URL = os.getenv("INFURA_URL")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

w3 = Web3(Web3.HTTPProvider(INFURA_URL))
engine = create_engine(DATABASE_URL)


def get_eth_balance(address: str) -> float:
    try:
        checksum = Web3.to_checksum_address(address)
        balance_wei = w3.eth.get_balance(checksum)
        return float(w3.from_wei(balance_wei, 'ether'))
    except Exception as e:
        print(f"[ETH Error] {address}: {e}")
        return 0.0


def get_token_balance(address: str, token_contract: str) -> float:
    url = (
        f"https://api.etherscan.io/api"
        f"?module=account&action=tokenbalance"
        f"&contractaddress={token_contract}"
        f"&address={address}"
        f"&tag=latest"
        f"&apikey={ETHERSCAN_API_KEY}"
    )
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        if data["status"] == "1":
            balance = int(data["result"])
            return balance / (10 ** 18)
        else:
            return 0.0
    except Exception as e:
        print(f"[Token Error] {address}: {e}")
        return 0.0


def enrich_balances(
    csv_path="addresses.csv",
    output_path="addresses_with_balances.csv",
    token_contract=None,
    eth_threshold=0.1
):
    df = pd.read_csv(csv_path)
    eth_balances = []
    token_balances = []

    print(f"💰 Получаем балансы для {len(df)} адресов...\n")

    for _, row in tqdm(df.iterrows(), total=len(df)):
        address = row["address"]
        eth = get_eth_balance(address)
        eth_balances.append(eth)

        token = None
        if token_contract:
            token = get_token_balance(address, token_contract)
        token_balances.append(token)

        time.sleep(0.2)

    df["eth_balance"] = eth_balances
    if token_contract:
        df["token_balance"] = token_balances

    # 📊 Фильтрация
    filtered_df = df[df["eth_balance"] > eth_threshold]
    print(f"\n🧮 Прошли фильтр (ETH > {eth_threshold}): {len(filtered_df)} адресов")

    # 💾 Сохраняем CSV
    filtered_df.to_csv(output_path, index=False)
    print(f"✅ CSV сохранён в: {output_path}")

    # 📥 Сохраняем в PostgreSQL
    try:
        filtered_df.to_sql("wallet_balances", engine, if_exists="replace", index=False)
        print(f"📥 Загружено в таблицу `wallet_balances`")
    except Exception as e:
        print(f"❌ Ошибка записи в БД: {e}")


if __name__ == "__main__":
    enrich_balances(token_contract=None)

