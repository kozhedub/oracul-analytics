import pandas as pd

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")


# Загружаем файл
df = pd.read_csv("export-tokenholders-for-contract-0x761d38e5ddf6ccf6cf7c55759d5210750b5d60f3.csv")

# Показываем первые строки
print(df.head())

# Проверим структуру
print(df.columns)
