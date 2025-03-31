import pandas as pd

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


# Загружаем файл
df = pd.read_csv("export-tokenholders-for-contract-0x761d38e5ddf6ccf6cf7c55759d5210750b5d60f3.csv")

# Показываем первые строки
print(df.head())

# Проверим структуру
print(df.columns)
