import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()  # Загружаем переменные окружения из .env

def get_db_url():
    return os.getenv("DATABASE_URL")

def get_engine():
    db_url = get_db_url()
    if not db_url:
        raise ValueError("DATABASE_URL не задан в .env")
    return create_engine(db_url)
