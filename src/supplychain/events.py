from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, field_validator

class ShipmentEvent(BaseModel):
    event_id: str = Field(min_length=1,max_length=128)
    shipment_id: str = Field(min_length=1,max_length=128)
    warehouse: str = Field(min_length=1,max_length=64)
    version: int = Field(gt=0,strict=True)
    quantity: int = Field(ge=0,strict=True)
    status: Literal['created','in_transit','delivered','cancelled']
    event_time: datetime

    @field_validator('event_time')
    @classmethod
    def timezone_required(cls,value):
        if value.tzinfo is None:
            raise ValueError('event_time needs a timezone')
        return value
