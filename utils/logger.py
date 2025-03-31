from rich.logging import RichHandler
import logging
import os
import sys

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


def setup_logger():
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("[%(asctime)s] | %(levelname)s | %(message)s"))

    # 🔧 Принудительно используем utf-8
    sys.stdout.reconfigure(encoding='utf-8')  # Python 3.7+

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [handler]
