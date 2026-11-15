import json
from pathlib import Path
from .store import EventStore

def main():
    Path('output').mkdir(exist_ok=True)
    store=EventStore('output/supplychain.sqlite')
    try:
        for index in range(30):
            store.apply({'event_id':f'local-{index}','shipment_id':f'S{index%10}',
                         'warehouse':f'W{index%3}','version':index//10+1,'quantity':index+1,
                         'status':'delivered' if index>=20 else 'in_transit',
                         'event_time':'2026-01-01T00:00:00Z'})
        print(json.dumps(store.summary(),indent=2))
    finally:
        store.close()
if __name__=='__main__':
    main()
