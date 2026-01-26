from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class OrderState(Enum):
    """Order state machine (simplified)"""
    PRE_ORDER_PLACED = "pre_order_placed"
    PAYMENT_PROCESSING = "payment_processing"
    AWAITING_RELEASE = "awaiting_release"
    FULFILLMENT_IN_PROGRESS = "fulfillment_in_progress"
    AWAITING_DELIVERY = "awaiting_delivery"
    DELIVERED = "delivered"
    REFUNDED = "refunded"


@dataclass
class PreOrder:
    """Pre-order information"""
    order_id: str
    customer_email: str
    product_name: str
    amount: float
    payment_method_id: str
    release_date: datetime


@dataclass
class CompensationRecord:
    """Tracks an action for potential compensation (Saga pattern)"""
    action: str
    resource_id: str