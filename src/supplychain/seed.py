from datetime import datetime,timezone
import json
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient,NewTopic

def main():
    config = {'bootstrap.servers':os.environ.get('KAFKA_BOOTSTRAP','localhost:9092')}
    admin = AdminClient(config)
    if 'shipment-events' not in admin.list_topics(timeout=15).topics:
        for future in admin.create_topics([NewTopic('shipment-events',3,1)]).values():
            future.result()
    producer = Producer(dict(config,**{'enable.idempotence':True,'acks':'all'}))
    failures = []
    def delivered(error,message):
        if error:
            failures.append(str(error))
    for index in range(100):
        payload = {'event_id':f'demo-{index}','shipment_id':f'ship-{index%20}',
                   'warehouse':f'warehouse-{index%3}','version':index//20+1,
                   'quantity':index+1,'status':'delivered' if index>=80 else 'in_transit',
                   'event_time':datetime.now(timezone.utc).isoformat()}
        producer.produce('shipment-events',key=payload['shipment_id'],value=json.dumps(payload),callback=delivered)
        producer.poll(0)
    pending = producer.flush(30)
    if failures or pending:
        raise RuntimeError(f'Delivery failed: {failures}; pending={pending}')

if __name__ == '__main__':
    main()
