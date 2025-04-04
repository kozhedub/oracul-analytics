import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, select, insert, desc, and_
from utils.logger import setup_logger

# 📄 Логгирование
setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

# 🌐 Переменные среды
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# 🔗 Подключение к БД
engine = create_engine(DATABASE_URL)
metadata = MetaData(bind=engine)
metadata.reflect()

# 📋 Таблицы
balances_table = metadata.tables.get("wallet_balances")
history_table = metadata.tables.get("wallet_asset_history")

if not balances_table or not history_table:
    logging.error("❌ Таблицы wallet_balances или wallet_asset_history не найдены")
    exit(1)

with engine.connect() as conn:
    result = conn.execute(select([
        balances_table.c.address,
        balances_table.c.balance_eth,
        balances_table.c.balance_token,
        balances_table.c.usd_value,
    ]))
    current_balances = result.fetchall()

    new_entries = 0
    for row in current_balances:
        address = row["address"]
        usd_value = float(row["usd_value"])

        # Получить последнее значение из истории
        latest_history = conn.execute(
            select(history_table.c.usd_value)
            .where(history_table.c.address == address)
            .order_by(desc(history_table.c.updated_at))
            .limit(1)
        ).fetchone()

        if latest_history is None or abs(usd_value - latest_history.usd_value) > 0.01:
            insert_stmt = insert(history_table).values(
                address=address,
                balance_eth=row["balance_eth"],
                balance_token=row["balance_token"],
                usd_value=usd_value,
                updated_at=datetime.now(timezone.utc)
            )
            conn.execute(insert_stmt)
            new_entries += 1

    logging.info(f"✅ История сохранена для {new_entries} адресов")
