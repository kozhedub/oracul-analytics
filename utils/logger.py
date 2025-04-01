import sys
import logging

def setup_logger():
    """
    Настраивает глобальный логгер, выводящий сообщения в stdout с уровнем INFO.
    Гарантирует корректную кодировку UTF-8.
    """
    # 🔧 Принудительно используем utf-8 (если доступно)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding='utf-8')

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("[%(asctime)s] | %(levelname)s | %(message)s"))

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Заменяем все предыдущие хендлеры новым
    root.handlers = [handler]
