from contextlib import contextmanager
import json
import sqlite3
from .events import ShipmentEvent

SCHEMA = [
  'CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,payload TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS shipments(shipment_id TEXT PRIMARY KEY,warehouse TEXT NOT NULL,version INTEGER NOT NULL,quantity INTEGER NOT NULL,status TEXT NOT NULL,event_time TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS rejected(source_key TEXT PRIMARY KEY,payload TEXT NOT NULL,reason TEXT NOT NULL)'
]

class EventStore:
    def __init__(self,url):
        self.postgres = url.startswith(('postgresql://','postgres://'))
        if self.postgres:
            import psycopg
            self.db = psycopg.connect(url,autocommit=True)
        else:
            self.db = sqlite3.connect(url.removeprefix('sqlite:///'),check_same_thread=False)
        with self.transaction():
            for statement in SCHEMA:
                self.db.execute(statement)

    @contextmanager
    def transaction(self):
        with (self.db.transaction() if self.postgres else self.db):
            yield

    def execute(self,sql,params=()):
        return self.db.execute(sql.replace('?','%s') if self.postgres else sql,params)

    def apply(self,payload):
        event = ShipmentEvent.model_validate(payload)
        serialized = event.model_dump_json()
        with self.transaction():
            inserted = self.execute('INSERT INTO events VALUES(?,?) ON CONFLICT(event_id) DO NOTHING',
                                    (event.event_id,serialized)).rowcount
            if not inserted:
                stored = self.execute('SELECT payload FROM events WHERE event_id=?',(event.event_id,)).fetchone()[0]
                if json.loads(stored) != json.loads(serialized):
                    raise ValueError('Conflicting event_id replay')
                return 'duplicate'
            self.execute('INSERT INTO shipments VALUES(?,?,?,?,?,?) ON CONFLICT(shipment_id) DO UPDATE SET warehouse=excluded.warehouse,version=excluded.version,quantity=excluded.quantity,status=excluded.status,event_time=excluded.event_time WHERE excluded.version>shipments.version',
                         (event.shipment_id,event.warehouse,event.version,event.quantity,event.status,event.event_time.isoformat()))
        return 'accepted'

    def reject(self,key,payload,reason):
        with self.transaction():
            self.execute('INSERT INTO rejected VALUES(?,?,?) ON CONFLICT(source_key) DO NOTHING',
                         (key,json.dumps(payload),str(reason)))

    def shipment(self,identifier):
        row = self.execute('SELECT shipment_id,warehouse,version,quantity,status,event_time FROM shipments WHERE shipment_id=?',(identifier,)).fetchone()
        return dict(zip(['shipment_id','warehouse','version','quantity','status','event_time'],row)) if row else None

    def summary(self):
        rows = self.execute('SELECT warehouse,status,COUNT(*),SUM(quantity) FROM shipments GROUP BY warehouse,status ORDER BY warehouse,status').fetchall()
        return [dict(zip(['warehouse','status','shipments','quantity'],r)) for r in rows]

    def close(self):
        self.db.close()
