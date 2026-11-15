import json
import pytest
from fastapi.testclient import TestClient
from supplychain.store import EventStore
from supplychain.worker import process
from supplychain.api import create_app

def event(**updates):
    return dict({'event_id':'e1','shipment_id':'s1','warehouse':'w1','version':1,
                 'quantity':12,'status':'created','event_time':'2026-01-01T00:00:00Z'},**updates)

def test_replay_stale_and_conflicting_events(tmp_path):
    path=str(tmp_path/'events.sqlite')
    store=EventStore(path)
    assert store.apply(event())=='accepted'
    assert store.apply(event())=='duplicate'
    store.apply(event(event_id='e2',version=3,status='delivered'))
    store.apply(event(event_id='e3',version=2,status='in_transit'))
    assert store.shipment('s1')['version']==3
    with pytest.raises(ValueError):
        store.apply(event(quantity=99))
    assert store.execute('SELECT COUNT(*) FROM events').fetchone()[0]==3
    store.close()
    reopened=EventStore(path)
    assert reopened.shipment('s1')['status']=='delivered'
    reopened.close()

def test_quarantine_and_sql_failure_do_not_ack(tmp_path,monkeypatch):
    store=EventStore(str(tmp_path/'sink.sqlite'))
    assert process(store,'partition:1',b'not json')=='rejected'
    assert process(store,'partition:2',json.dumps(event(quantity=-1)))=='rejected'
    assert store.execute('SELECT COUNT(*) FROM rejected').fetchone()[0]==2
    def unavailable(payload):
        raise ConnectionError('database unavailable')
    monkeypatch.setattr(store,'apply',unavailable)
    with pytest.raises(ConnectionError):
        process(store,'partition:3',json.dumps(event()))
    store.close()

def test_api(tmp_path):
    url=str(tmp_path/'api.sqlite')
    with TestClient(create_app(url)) as client:
        assert client.get('/health').status_code==200
        assert client.get('/shipments/unknown').status_code==404
        assert client.get('/warehouses').json()==[]
