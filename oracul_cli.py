import argparse
import subprocess
from utils.telegram import send_telegram_message

import logging
from utils.logger import setup_logger

setup_logger()
logging.info(f"📄 Запущен скрипт: {__file__}")

def run_command(description, command):
    try:
        print(f"\n🚀 {description}...")
        subprocess.run(command, check=True)
        print(f"✅ {description} — Успешно")
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} — Ошибка: {e}")
        send_telegram_message(f"❌ {description} — Ошибка при выполнении")


def main():
    parser = argparse.ArgumentParser(description="🧠 Oracul CLI Commander")

    parser.add_argument("--collect", action="store_true", help="Собрать транзакции по адресам")
    parser.add_argument("--balances", action="store_true", help="Обновить балансы адресов")
    parser.add_argument("--meta", action="store_true", help="Обновить wallets_meta")
    parser.add_argument("--prices", action="store_true", help="Обновить курсы токенов")
    parser.add_argument("--cluster", action="store_true", help="Запустить кластеризацию")
    parser.add_argument("--whales", action="store_true", help="Whale alerts")
    parser.add_argument("--all", action="store_true", help="Запустить весь Oracul-пайплайн")

    args = parser.parse_args()

    if args.all or args.collect:
        run_command("Сбор транзакций", ["python", "etl/oracul_sprint_setup.py"])

    if args.balances:
        run_command("Обновление балансов", ["python", "etl/balance_updater.py"])

    if args.meta:
        run_command("Обновление wallets_meta", ["python", "etl/wallets_meta_updater.py"])

    if args.prices:
        run_command("Обновление цен токенов", ["python", "utils/prices.py"])

    if args.cluster:
        run_command("Кластеризация", ["python", "clustering/wallet_clusterer.py"])

    if args.whales:
        run_command("Whale Alerts", ["python", "tools/whale_alerts.py"])


if __name__ == "__main__":
    main()
