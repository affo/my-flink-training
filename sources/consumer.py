import sys
from config import CONF
from kafka import KafkaConsumer, TopicPartition

if len(sys.argv) == 1:
    print "Pass topic name in, please"
    sys.exit(1)

host = CONF.get('DEFAULT', 'kafka_host')
port = CONF.get('DEFAULT', 'kafka_port')
broker = host + ':' + port
topic = sys.argv[1]
partition = 0

consumer = KafkaConsumer(bootstrap_servers=broker)
tp = TopicPartition(topic, partition)
consumer.assign([tp, ])

consumer.seek_to_beginning(tp)
for msg in consumer:
    print '>>>', msg

