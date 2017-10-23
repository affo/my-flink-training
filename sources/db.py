import redis
from config import CONF

def _update_handler(message):
    print '\nUPDATE >>>', message['data'], '\n'


class Updater(object):
    updates_topic = 'UPDATES'

    def __init__(self, update_handler=_update_handler):
        host = CONF.get('DEFAULT', 'redis_host')
        port = CONF.getint('DEFAULT', 'redis_port')

        self.cli = redis.StrictRedis(host=host, port=port, db=0)
        self.pubsub = self.cli.pubsub()
        self.update_handler = update_handler

    def update(self, key, value):
        return self.cli.pipeline() \
                    .set(key, value) \
                    .publish(self.updates_topic, '{}:{}'.format(key, value)) \
                    .execute()

    def run_updates_listener(self):
        self.pubsub.subscribe(**{self.updates_topic: self.update_handler})
        self.listener = self.pubsub.run_in_thread(sleep_time=0.001)

    def stop_listener(self):
        self.listener.stop()

