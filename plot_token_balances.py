import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from dotenv import load_dotenv
from utils.telegram import send_telegram_photo

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")

load_dotenv()

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

# 🔗 Подключение к БД
engine = create_engine(os.getenv("DATABASE_URL"))

# 📦 Получаем все данные по токенам
query = """
SELECT address, token_symbol, balance, updated_at
FROM wallet_token_balances
ORDER BY updated_at;
"""

df = pd.read_sql(query, engine)
df["updated_at"] = pd.to_datetime(df["updated_at"])

# 📂 Папка для сохранения графиков
os.makedirs("plots", exist_ok=True)

# 🔄 По каждому токену — строим график
for token in df["token_symbol"].unique():
    df_token = df[df["token_symbol"] == token]
    plt.figure(figsize=(10, 5))

    for address in df_token["address"].unique():
        df_addr = df_token[df_token["address"] == address]
        plt.plot(df_addr["updated_at"], df_addr["balance"], label=address)

    plt.title(f"📊 Баланс токена {token} по адресам")
    plt.xlabel("Дата")
    plt.ylabel(f"Количество {token}")
    plt.legend(fontsize="small")
    plt.grid(True)
    plt.tight_layout()

    plot_path = f"plots/{token}_balances.png"
    plt.savefig(plot_path)
    plt.close()

    print(f"📈 Сохранён график: {plot_path}")
    send_telegram_photo(plot_path, f"📊 График баланса токена {token} по адресам")

print("✅ Все графики сохранены и отправлены в Telegram.")
