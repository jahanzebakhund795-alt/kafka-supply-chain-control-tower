from contextlib import asynccontextmanager
import os
from fastapi import FastAPI,HTTPException
from .store import EventStore

def create_app(database_url=None):
    @asynccontextmanager
    async def lifespan(app):
        app.state.store = EventStore(database_url or os.environ['DATABASE_URL'])
        yield
        app.state.store.close()
    app = FastAPI(title='Supply chain control tower',lifespan=lifespan)
    @app.get('/health')
    def health():
        app.state.store.execute('SELECT 1')
        return {'status':'ready'}
    @app.get('/shipments/{shipment_id}')
    def shipment(shipment_id:str):
        value = app.state.store.shipment(shipment_id)
        if value is None:
            raise HTTPException(404,'Unknown shipment')
        return value
    @app.get('/warehouses')
    def warehouses():
        return app.state.store.summary()
    return app
