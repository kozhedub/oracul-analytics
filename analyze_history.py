import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from utils.telegram import send_telegram_photo

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

# 📂 Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.getenv("ADDRESS_CSV_PATH", os.path.join(BASE_DIR, "../data/addresses.csv"))

# 📊 Запрос для анализа накоплений и распродаж
query = """
SELECT
    address,
    token_symbol,
    updated_at,
    balance,
    LAG(balance) OVER (PARTITION BY address, token_symbol ORDER BY updated_at) AS prev_balance
FROM wallet_token_balances
ORDER BY token_symbol, address, updated_at;
"""

df = pd.read_sql(text(query), engine)
df["updated_at"] = pd.to_datetime(df["updated_at"])
df["delta"] = df["balance"] - df["prev_balance"]

# 📊 Строим графики по токенам
os.makedirs("plots/history", exist_ok=True)

for token in df["token_symbol"].dropna().unique():
    df_token = df[df["token_symbol"] == token]

    # Только адреса с изменениями
    changed = df_token[df_token["delta"] != 0]
    if changed.empty:
        continue

    top_addresses = (
        changed.groupby("address")["delta"].sum().abs().sort_values(ascending=False).head(5).index
    )

    plt.figure(figsize=(10, 5))
    for address in top_addresses:
        df_addr = df_token[df_token["address"] == address]
        plt.plot(df_addr["updated_at"], df_addr["balance"], label=address[:6] + "...")

    plt.title(f"📊 Изменения баланса: {token}")
    plt.xlabel("Дата")
    plt.ylabel(token)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    path = f"plots/history/{token}_history.png"
    plt.savefig(path)
    plt.close()

    print(f"📈 {token} график сохранен: {path}")
    send_telegram_photo(path, f"📊 Баланс по токену {token}")

print("✅ Исторические графики построены и отправлены.")
