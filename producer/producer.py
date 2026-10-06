import sys
import csv
import json
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer


FIELDS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon", "target",
)
NUMERIC_FIELDS = {
    "amount": float,
    "post_code": int,
    "lat": float,
    "lon": float,
    "population_city": int,
    "merchant_lat": float,
    "merchant_lon": float,
    "target": int,
}


def parse_row(row, line_number):
    try:
        event = {field: row[field].strip() for field in FIELDS}
        event["transaction_time"] = datetime.fromisoformat(
            event["transaction_time"]
        ).strftime("%Y-%m-%d %H:%M:%S")
        for field, convert in NUMERIC_FIELDS.items():
            event[field] = convert(event[field])
        return event
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Ошибка в строке CSV {line_number}: {exc}") from exc


def send_csv(path):
    producer = Producer({"bootstrap.servers": "kafka:9092", "acks": "all"}) # подключение к кафке
    failures = []

    def on_delivery(error, _message):
        # функция для выявления первой ошибки доставки в kafka (_message не используется)
        if error is not None and not failures:
            failures.append(str(error))

    count = 0
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)

        for line_number, row in enumerate(reader, start=2): # пробегаемся по строкам (start=2 для логов при ошибке)
            event = parse_row(row, line_number)
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    producer.produce("transactions", value=payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(1)
            count += 1
            producer.poll(0)

    remaining = producer.flush(60) # можно поставить другое времся ожидания
    if remaining or failures:
        raise RuntimeError(f"Не доставлено сообщений: {remaining}; ошибка: {failures[:1]}")
    print(f"kafka получила: {count}")


if __name__ == "__main__":
    # эта штука как раз выполнит фунцию send_csv
    # есдли не указывать путь, то по дефолту берется /data/train.csv
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/data/train.csv")
    send_csv(path)
