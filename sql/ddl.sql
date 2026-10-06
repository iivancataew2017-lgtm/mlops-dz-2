CREATE DATABASE IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.transactions_kafka
(
    transaction_time DateTime,
    merch String,
    cat_id String,
    amount Decimal(12, 2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code UInt32,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse-transactions',
    kafka_format = 'JSONEachRow'
;

CREATE TABLE IF NOT EXISTS fraud.transactions
(
    transaction_time DateTime,
    merch LowCardinality(String),
    cat_id LowCardinality(String) CODEC(ZSTD(1)),
    amount Decimal(12, 2) CODEC(ZSTD(1)),
    name_1 String,
    name_2 String,
    gender LowCardinality(String),
    street String,
    one_city LowCardinality(String),
    us_state LowCardinality(String),
    post_code UInt32,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs LowCardinality(String),
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
)
ENGINE = MergeTree
ORDER BY
    (us_state, transaction_time)
;

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.kafka_to_transactions
TO fraud.transactions
AS
SELECT
    *
FROM
    fraud.transactions_kafka
;
