import os
import pandas as pd
from dotenv import load_dotenv
import requests
import time


import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")


load_dotenv()
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

def get_transactions(address, start_block=0, end_block=99999999):
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "sort": "asc",
        "apikey": API_KEY
    }
    res = requests.get(url, params=params)
    return res.json()

def get_erc20_transactions(address):
    API_KEY = os.getenv("ETHERSCAN_API_KEY") or "demo"
    base_url = "https://api.etherscan.io/api"

    retries = int(os.getenv("ETHERSCAN_RETRIES", 3))
    delay = int(os.getenv("ETHERSCAN_DELAY", 5))
    timeout = int(os.getenv("ETHERSCAN_TIMEOUT", 20))

    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(base_url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            # Проверяем статус ответа
            if data.get("status") == "1" and "result" in data:
                return pd.DataFrame(data["result"])
            else:
                logging.warning(f"⚠️ Нет данных или статус != 1 для {address}")
                return pd.DataFrame()  # Возвращаем пустой DataFrame

        except requests.exceptions.RequestException as e:
            logging.warning(f"⚠️ {address} | Попытка {attempt}/{retries} — {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"❌ {address} | Все попытки неудачны.")
                return pd.DataFrame()
def get_token_balances(address):
    url = (
        f"https://api.etherscan.io/api"
        f"?module=account"
        f"&action=tokentx"
        f"&address={address}"
        f"&startblock=0"
        f"&endblock=99999999"
        f"&sort=desc"
        f"&apikey={ETHERSCAN_API_KEY}"
    )

    try:
        response = requests.get(url)
        data = response.json()
        if data["status"] != "1":
            return []

        # Группируем последние балансы по токенам
        seen = {}
        for tx in data["result"]:
            contract = tx["contractAddress"]
            if contract not in seen:
                seen[contract] = tx  # Сохраняем только последний tx

        return list(seen.values())

    except Exception as e:
        print(f"Ошибка получения токенов для {address}: {e}")
        return []
