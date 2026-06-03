from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, field_validator
import uuid


class OrderPlaced(BaseModel):
    event_type: Literal["order_placed"] = "order_placed"
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    product_id: str
    category: Literal["electronics", "clothing", "home", "sports", "books", "beauty"]
    quantity: int = Field(ge=1, le=100)
    price: float = Field(gt=0)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("price")
    @classmethod
    def round_price(cls, v: float) -> float:
        return round(v, 2)


class OrderShipped(BaseModel):
    event_type: Literal["order_shipped"] = "order_shipped"
    order_id: str
    carrier: Literal["UPS", "FedEx", "USPS", "DHL"]
    tracking_number: str
    estimated_delivery: datetime
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("estimated_delivery")
    @classmethod
    def delivery_must_be_future(cls, v: datetime) -> datetime:
        if v <= datetime.utcnow():
            raise ValueError("estimated_delivery must be in the future")
        return v


class PageView(BaseModel):
    event_type: Literal["page_view"] = "page_view"
    session_id: str
    customer_id: str
    product_id: str
    page_url: str
    device_type: Literal["desktop", "mobile", "tablet"]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
