# 💡 Oracul: Onchain Intelligence System

### 📁 Структура проекта

```bash
oracul_eth_data/
├── etl/                     # Основные ETL-пайплайны
│   ├── oracul_sprint_setup.py   # Главный пайплайн запуска анализа
│   ├── balance_updater.py       # Обновление балансов адресов
│   ├── wallets_meta_updater.py # Обновление метаинформации об адресах
│   ├── whale_alerts.py         # Уведомления по крупным транзакциям (встроено в sprint)
│   └── scheduler_oracul.py     # Запуск старых адресов по расписанию
│
├── clustering/             # Кластеризация адресов
│   └— wallet_clusterer.py     # Выявление кластеров с использованием графа
│
├── tools/                  # Утилиты и вспомогательные скрипты
│   ├── get_eth_token_balances.py # Получение токенов через Infura
│   └— inspect_holders.py       # Анализ выгрузок токенов
│
├── utils/                  # Общие функции и сервисы
│   ├── etherscan.py            # Работа с API Etherscan
│   ├── infura.py               # Работа с Infura
│   ├── logger.py               # Логгирование с цветами
│   ├── prices.py               # Курсы токенов
│   ├── scheduler_utils.py      # Логика фильтрации старых адресов
│   └— telegram.py             # Telegram-уведомления
│
├── scripts/               # Сервисные скрипты для пуска/подготовки
│   └— create_tables.py         # Создает таблицы token_prices и wallets_meta
│
├── addresses.csv           # Входной список адресов (с метками)
├── processed.log           # Журнал обработки
├── .env                    # Переменные окружения (API-ключи и PostgreSQL URL)
└— README.md               # Документация проекта
```

...