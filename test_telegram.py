from utils.telegram import send_telegram_message

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")


send_telegram_message("✅ Тестовое сообщение из Oracul!")
