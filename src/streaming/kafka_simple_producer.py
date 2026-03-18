# -*- coding: utf-8 -*-

import json
from kafka import KafkaProducer
from binance import Client
from binance import ThreadedWebsocketManager

from datetime import datetime

# Configuración
BOOTSTRAP_SERVERS="51.49.235.244:9092"
USERNAME="kafka_client"
PASSWORD="88b8a35dca1a04da57dc5f3e"
TOPIC="imat3a_ETH"
KEY = "ETHUSDT"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8")
)

def ms2utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def handle_kline(msg):
    
    global producer

    k = msg['k']
    if k['x']:
        key = k['s']
        value = {
            "symbol": key,
            "@timestamp": ms2utc(k['T']),
            "close": k['c'],
            "volume": k['v']
        }
        timestamp = k['T']
        producer.send(TOPIC, key=key, value=value, timestamp_ms=timestamp)
        


def main() -> None:

    # Crea el KafkaProducer

    global producer
    print("Creando hilo...")
    twm = ThreadedWebsocketManager()
    twm.start()
    print("Arrancando hilo...")
    while True:
        twm.start_kline_socket(
            symbol=KEY,
            interval=Client.KLINE_INTERVAL_1MINUTE,
            callback=handle_kline
        )

        if input("Pulsa ENTER para salir\n") == "":
            break
    print("Parando hilo...")
    twm.stop()

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()