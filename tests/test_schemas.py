import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from schemas import OrderPlaced, OrderShipped, PageView


# ── OrderPlaced ───────────────────────────────────────────────────────────────

class TestOrderPlaced:
    def test_valid_order(self):
        order = OrderPlaced(
            customer_id="cust_1",
            product_id="prod_1",
            category="electronics",
            quantity=2,
            price=99.99,
        )
        assert order.event_type == "order_placed"
        assert order.price == 99.99
        assert order.order_id is not None

    def test_price_is_rounded(self):
        order = OrderPlaced(
            customer_id="cust_1", product_id="prod_1",
            category="books", quantity=1, price=9.999,
        )
        assert order.price == 10.0

    def test_invalid_price_zero(self):
        with pytest.raises(ValidationError):
            OrderPlaced(
                customer_id="cust_1", product_id="prod_1",
                category="electronics", quantity=1, price=0,
            )

    def test_invalid_price_negative(self):
        with pytest.raises(ValidationError):
            OrderPlaced(
                customer_id="cust_1", product_id="prod_1",
                category="electronics", quantity=1, price=-5.0,
            )

    def test_quantity_too_high(self):
        with pytest.raises(ValidationError):
            OrderPlaced(
                customer_id="cust_1", product_id="prod_1",
                category="electronics", quantity=101, price=10.0,
            )

    def test_quantity_zero(self):
        with pytest.raises(ValidationError):
            OrderPlaced(
                customer_id="cust_1", product_id="prod_1",
                category="electronics", quantity=0, price=10.0,
            )

    def test_invalid_category(self):
        with pytest.raises(ValidationError):
            OrderPlaced(
                customer_id="cust_1", product_id="prod_1",
                category="furniture", quantity=1, price=10.0,
            )

    def test_all_valid_categories(self):
        for cat in ["electronics", "clothing", "home", "sports", "books", "beauty"]:
            order = OrderPlaced(
                customer_id="cust_1", product_id="prod_1",
                category=cat, quantity=1, price=10.0,
            )
            assert order.category == cat


# ── OrderShipped ──────────────────────────────────────────────────────────────

class TestOrderShipped:
    def test_valid_shipment(self):
        shipment = OrderShipped(
            order_id="order_123",
            carrier="UPS",
            tracking_number="UPS1234567890",
            estimated_delivery=datetime.utcnow() + timedelta(days=3),
        )
        assert shipment.event_type == "order_shipped"
        assert shipment.carrier == "UPS"

    def test_invalid_carrier(self):
        with pytest.raises(ValidationError):
            OrderShipped(
                order_id="order_123",
                carrier="Amazon",
                tracking_number="AMZ123",
                estimated_delivery=datetime.utcnow() + timedelta(days=3),
            )

    def test_delivery_in_past_rejected(self):
        with pytest.raises(ValidationError):
            OrderShipped(
                order_id="order_123",
                carrier="FedEx",
                tracking_number="FDX123",
                estimated_delivery=datetime.utcnow() - timedelta(days=1),
            )

    def test_all_valid_carriers(self):
        for carrier in ["UPS", "FedEx", "USPS", "DHL"]:
            s = OrderShipped(
                order_id="order_1",
                carrier=carrier,
                tracking_number="TRACK123",
                estimated_delivery=datetime.utcnow() + timedelta(days=2),
            )
            assert s.carrier == carrier


# ── PageView ──────────────────────────────────────────────────────────────────

class TestPageView:
    def test_valid_page_view(self):
        pv = PageView(
            session_id="sess_abc",
            customer_id="cust_1",
            product_id="prod_42",
            page_url="/products/prod_42",
            device_type="mobile",
        )
        assert pv.event_type == "page_view"
        assert pv.device_type == "mobile"

    def test_invalid_device_type(self):
        with pytest.raises(ValidationError):
            PageView(
                session_id="sess_abc",
                customer_id="cust_1",
                product_id="prod_42",
                page_url="/products/prod_42",
                device_type="smartwatch",
            )

    def test_all_valid_device_types(self):
        for device in ["desktop", "mobile", "tablet"]:
            pv = PageView(
                session_id="sess_1",
                customer_id="cust_1",
                product_id="prod_1",
                page_url="/products/prod_1",
                device_type=device,
            )
            assert pv.device_type == device
