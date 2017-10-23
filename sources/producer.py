import time, random
from kafka import KafkaProducer
from config import CONF

host = CONF.get('DEFAULT', 'kafka_host')
port = CONF.get('DEFAULT', 'kafka_port')
broker = host + ':' + port
topic = CONF.get('DEFAULT', 'events_topic')
partition = 0

producer = KafkaProducer(bootstrap_servers=broker)

keys = ['foo', 'bar', 'buz']
while True:
    msg = random.choice(keys)
    print '>>>', msg
    producer.send(topic, msg, partition=partition).get(timeout=60)
    time.sleep(1)

