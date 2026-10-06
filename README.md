# Домашнее задание 2

## Что делал по пунктам задания

1) `producer/producer.py` читает `data/train.csv` и отправляет транзакции в топик `transactions`.

2) `sql/ddl.sql` создаёт таблицу для чтения сообщений из Kafka и таблицу для хранения всех транзакций. Новые сообщения автоматически попадают в таблицу `fraud.transactions`.

3) `sql/max_by_state.sql` просматривает транзакции и для каждого штата находит категорию самой крупной покупки и её сумму. Результат для полного `train.csv` сохранён в `results/max_by_state.csv`.

4) Для хранения использованы приёмы из лекции: `LowCardinality` для повторяющихся строк, сортировка по штату и времени через `ORDER BY`, а также `CODEC(ZSTD(1))` для категории и суммы. Благодаря этому нужные запросу столбцы занимают меньше места. Партиции по месяцам здесь не добавлены: запрос ищет максимумы за всё время и не отбирает данные по дате.

## Запуск

Нужны Docker Desktop с запущенным движком и Docker Compose v2. Все команды выполняются из каталога с `docker-compose.yml`.

Если вы уже запускали старую версию проекта, сначала выполните `docker compose down -v`, чтобы ClickHouse создал таблицы по обновлённому DDL. Эта команда удалит ранее загруженные данные, поэтому CSV нужно будет отправить повторно.

Скачайте [датасет для ДЗ-2](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394) и положите файл в `data/train.csv`. CSV не включён в репозиторий. Если файл с теми же столбцами называется иначе, укажите его путь при запуске: `docker compose run --rm producer /data/cool.csv`.

```bash
docker compose up -d --build
docker compose run --rm producer
```

Для полного `train.csv` запрос ниже должен вернуть `786431`. Повторный запуск producer добавит те же строки ещё раз.

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM fraud.transactions"
```

## Запрос и результат

```bash
docker compose exec -T clickhouse clickhouse-client --queries-file /query.sql > results/max_by_state.csv
```

Запрос возвращает штат, категорию и сумму максимальной транзакции; таблица сохраняется в `results/max_by_state.csv`.

## Как устроено хранение

`sql/ddl.sql` создаёт таблицу Kafka Engine и основную таблицу `MergeTree`. В основной таблице повторяющиеся строки хранятся как `LowCardinality(String)`, транзакции сортируются по штату и времени, а столбцы категории и суммы дополнительно сжимаются через `CODEC(ZSTD(1))`. Итоговый запрос читает эти данные напрямую.

## Структура ДЗ-2

```text
./
├── docker-compose.yml
├── requirements.txt
├── producer/
│   ├── Dockerfile
│   └── producer.py
├── sql/
│   ├── ddl.sql
│   └── max_by_state.sql
├── data/
│   ├── .gitkeep
│   └── train.csv # скачивается отдельно
├── results/
│   └── max_by_state.csv
├── .dockerignore
├── .gitignore
└── README.md
```
