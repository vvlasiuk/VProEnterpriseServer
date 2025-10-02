# Показати доступні команди
python -m app.cli --help

# Перевірити стан БД
python -m app.cli status

# Показати схему
python -m app.cli schema

# Застосувати міграції
python -m app.cli migrate

# Деталі конкретної таблиці
python -m app.cli show-table users

# Створити таблиці
python -m app.cli migrate

# Видалити зайві таблиці
python -m app.cli drop-extra-tables

# Очистити всю БД (НЕБЕЗПЕЧНО!)
python -m app.cli clean-database

# Примусово без підтвердження
python -m app.cli drop-extra-tables --force

# Повна синхронізація (очистити + створити)
python -m app.cli clean-database --force && python -m app.cli migrate