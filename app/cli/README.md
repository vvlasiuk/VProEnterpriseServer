<!-- Порядок оновлення -->
# створити таблиці
python -m app.cli migrate 
# видалити таблиці
python -m app.cli drop-extra-tables
# оновити колонки
python -m app.cli migrate --update-existing
<!-- Порядок оновлення -->

# Показати доступні команди
python -m app.cli --help

# Перевірити стан БД
python -m app.cli status

# Показати схему
python -m app.cli schema

# Застосувати міграції, Створити таблиці
python -m app.cli migrate

# Деталі конкретної таблиці
python -m app.cli show-table users

# Видалити зайві таблиці
python -m app.cli drop-extra-tables

# Очистити всю БД (НЕБЕЗПЕЧНО!)
python -m app.cli clean-database

# Примусово без підтвердження
python -m app.cli drop-extra-tables --force

# Повна синхронізація (очистити + створити)
python -m app.cli clean-database --force && python -m app.cli migrate

# Показати що буде змінено
python -m app.cli migrate --dry-run --update-existing

# Оновити існуючі таблиці
python -m app.cli migrate --update-existing

# Показати різниці для конкретної таблиці
python -m app.cli diff-table users

# Створити нові + оновити існуючі
python -m app.cli migrate --update-existing

# Тільки заповнення даними
python -m app.cli seed

# Міграція + заповнення
python -m app.cli migrate-and-seed

# Повна синхронізація
python -m app.cli clean-database --force && python -m app.cli migrate-and-seed