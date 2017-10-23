import time
from db import Updater
from kafka import KafkaProducer
from config import CONF

host = CONF.get('DEFAULT', 'kafka_host')
port = CONF.get('DEFAULT', 'kafka_port')
broker = host + ':' + port
topic = CONF.get('DEFAULT', 'updates_topic')
partition = 0

producer = KafkaProducer(bootstrap_servers=broker)

def forward_to_kafka(message):
    producer.send(topic, message['data'], partition=partition).get(timeout=60)

u = Updater(update_handler=forward_to_kafka)

u.run_updates_listener()

try:
    while True:
        key = raw_input('KEY:\t')
        value = raw_input('VAL:\t')
        u.update(key, value)
        time.sleep(0.1)
except KeyboardInterrupt:
    print 'Bye'
finally:
    u.stop_listener()

