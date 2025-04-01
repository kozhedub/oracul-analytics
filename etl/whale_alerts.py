import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from utils.telegram import send_telegram_message

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")


# 📦 Настройка окружения
load_dotenv()


# Подключение к БД
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# 🧠 Whale Alert: проверяем резкие изменения
query = text("""
    SELECT address, token_symbol,
           MAX(balance) - MIN(balance) AS change
    FROM wallet_token_balances
    WHERE updated_at >= NOW() - INTERVAL '1 day'
    GROUP BY address, token_symbol
    HAVING ABS(MAX(balance) - MIN(balance)) > 1000000
    ORDER BY change DESC
    LIMIT 10;
""")


logging.info("🔍 Ищем резкие изменения баланса токенов")

try:
    with engine.begin() as conn:
        result = conn.execute(query).mappings()
        for row in result:
            addr = row["address"]
            change = row["change"]
            symbol = row["token_symbol"]

            message = f"🐋 Whale Alert:\nАдрес: {addr}\nТокен: {symbol}\nИзменение: {change:.2f}"
            send_telegram_message(message)
            logging.info(f"📨 Отправлено сообщение по {symbol}")
except Exception as e:
    logging.error(f"❌ Ошибка в whale_alerts: {e}")
