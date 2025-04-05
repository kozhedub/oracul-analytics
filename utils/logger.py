import sys
import logging
import os


def setup_logger(log_filename="oracul.log"):
    # Определим базовую директорию
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Папка для логов — data/
    log_dir = os.path.join(BASE_DIR, "data")
    os.makedirs(log_dir, exist_ok=True)

    # Полный путь к файлу лога
    log_path = os.path.join(log_dir, log_filename)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="a", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
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
