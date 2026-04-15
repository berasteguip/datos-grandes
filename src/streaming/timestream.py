#!/usr/bin/env python3
"""
Prueba minima de escritura en Amazon Timestream.
Escribe un registro de prueba en las tablas de Timestream:
- btc_quotes_raw -> En esta tabla vamos a meter el parámetro de la cotización o el valor "close"
- btc_vwap_5m -> n esta tabla vamos a meter el kpi "vwap"
"""

import json
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import time
from datetime import datetime, timezone

import boto3

# Parametros de prueba (edita estos valores para poner los correspondientes a vuestro grupo)
REGION = "eu-west-1"
DATABASE = "imat3a_crypto_rt"
QUOTES_TABLE = "eth"
VWAP_TABLE = "eth_vwap"
SYMBOL = "ETHUSDT"
WINDOW_START = "2026-03-25T15:10:00.000Z"
WINDOW_END = "2026-03-25T15:15:00.000Z"

QUOTES_TOPIC = "imat3a_ETH"
VWAP_TOPIC = "imat3a_ETH_VWAP"

BOOTSTRAP_SERVERS="51.49.235.244:9092"
USERNAME="kafka_client"
PASSWORD="88b8a35dca1a04da57dc5f3e"
GROUP_ID="imat3a_group1"


def now_epoch_ms() -> str:
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


def iso_to_epoch_ms(value: str) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))


def get_first_measure(records: dict, measure_name: str):
    """Return the first value found for a given field in a Kafka poll result."""
    if not records:
        print(f"No Kafka messages received while looking for {measure_name}.")
        return None

    for _, consumer_records in records.items():
        for consumer_record in consumer_records:
            print(
                f"Received Kafka message: topic={consumer_record.topic}, "
                f"partition={consumer_record.partition}, "
                f"offset={consumer_record.offset}, "
                f"key={consumer_record.key}, "
                f"value={consumer_record.value}"
            )
            value = consumer_record.value.get(measure_name)
            if value is not None:
                return value

    print(f"Kafka messages received, but {measure_name} value was not found.")
    return None


def assign_all_partitions(consumer: KafkaConsumer, topic: str) -> None:
    """Assign a consumer to every partition of a topic."""
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise RuntimeError(f"No partitions found for Kafka topic {topic}.")

    topic_partitions = [
        TopicPartition(topic, partition)
        for partition in sorted(partitions)
    ]
    consumer.assign(topic_partitions)
    print(f"Assigned {topic} partitions: {topic_partitions}")


def send_to_timestream(ts: boto3.client, quote_value: float, vwap_value: float) -> None:
    """
    Sends quote and VWAP values of coin to Timestream DB

    Args:
        ts (boto3.client): Timestream client
        quote_value (float): _value of the quote to be sent to Timestream
        vwap_value (float): _value of the VWAP to be sent to Timestream
    Returns:
        None
    """
    quote_record = {
        "Dimensions": [
            {"Name": "symbol", "Value": SYMBOL},
            {"Name": "source_topic", "Value": QUOTES_TOPIC},
            {"Name": "window_start", "Value": WINDOW_START},
            {"Name": "window_end", "Value": WINDOW_END},
            {"Name": "event_ts", "Value": WINDOW_END},
        ],
        "MeasureName": "close",
        "MeasureValue": str(float(quote_value)),
        "MeasureValueType": "DOUBLE",
        "Time": iso_to_epoch_ms(WINDOW_END),
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000),
    }

    vwap_record = {
        "Dimensions": [
            {"Name": "symbol", "Value": SYMBOL},
            {"Name": "window_start", "Value": WINDOW_START},
            {"Name": "window_end", "Value": WINDOW_END},
            {"Name": "source_topic", "Value": VWAP_TOPIC},
        ],
        "MeasureName": "vwap",
        "MeasureValue": str(float(vwap_value)),
        "MeasureValueType": "DOUBLE",
        "Time": iso_to_epoch_ms(WINDOW_END),
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000) + 1,
    }

    quote_resp = ts.write_records(
        DatabaseName=DATABASE,
        TableName=QUOTES_TABLE,
        Records=[quote_record],
    )
    vwap_resp = ts.write_records(
        DatabaseName=DATABASE,
        TableName=VWAP_TABLE,
        Records=[vwap_record],
    )

    print("Write OK (2 tablas)")
    print(f"Database={DATABASE} Region={REGION}")
    print(f"Tabla {QUOTES_TABLE} record:")
    print(json.dumps(quote_record, indent=2))
    print("Response:")
    print(json.dumps(quote_resp, default=str, indent=2))
    print(f"Tabla {VWAP_TABLE} record:")
    print(json.dumps(vwap_record, indent=2))
    print("Response:")
    print(json.dumps(vwap_resp, default=str, indent=2))      


def main() -> None:
    ts = boto3.client("timestream-write", region_name=REGION)

    quotes_consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8"),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    vwap_consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8"),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    assign_all_partitions(quotes_consumer, QUOTES_TOPIC)
    assign_all_partitions(vwap_consumer, VWAP_TOPIC)

    try:
        last_quote_value = None
        last_vwap_value = None

        while True:
            quote_value = get_first_measure(
                quotes_consumer.poll(timeout_ms=1000, max_records=1),
                "close",
            )
            vwap_value = get_first_measure(
                vwap_consumer.poll(timeout_ms=1000, max_records=1),
                "vwap",
            )

            print(quotes_consumer.bootstrap_connected(), flush=True)
            print(vwap_consumer.bootstrap_connected(), flush=True)

            if quote_value is not None:
                last_quote_value = quote_value
            if vwap_value is not None:
                last_vwap_value = vwap_value

            if last_quote_value is None or last_vwap_value is None:
                continue

            send_to_timestream(ts, last_quote_value, last_vwap_value)
            last_quote_value = None
            last_vwap_value = None
    except KeyboardInterrupt:
        print("Stopping consumers...")
    finally:
        quotes_consumer.close()
        vwap_consumer.close()


if __name__ == "__main__":
    main()
