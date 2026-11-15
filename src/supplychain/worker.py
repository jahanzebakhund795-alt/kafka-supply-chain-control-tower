import json
import os
from confluent_kafka import Consumer, KafkaException
from pydantic import ValidationError
from .store import EventStore

def process(store,source_key,raw):
    try:
        payload = json.loads(raw)
        return store.apply(payload)
    except (json.JSONDecodeError,ValidationError,ValueError) as error:
        store.reject(source_key,raw.decode(errors='replace') if isinstance(raw,bytes) else raw,str(error))
        return 'rejected'

def main():
    store = EventStore(os.environ['DATABASE_URL'])
    consumer = Consumer({'bootstrap.servers':os.environ.get('KAFKA_BOOTSTRAP','localhost:9092'),
                         'group.id':'supply-chain-v1','enable.auto.commit':False,
                         'enable.auto.offset.store':False,'auto.offset.reset':'earliest'})
    consumer.subscribe(['shipment-events'])
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            key = f'{message.topic()}:{message.partition()}:{message.offset()}'
            result = process(store,key,message.value())
            # SQL commit or durable rejection must precede offset advancement.
            consumer.commit(message=message,asynchronous=False)
            print(json.dumps({'source':key,'result':result}),flush=True)
    finally:
        consumer.close()
        store.close()

if __name__ == '__main__':
    main()
