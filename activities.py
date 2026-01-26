import random
import uuid
import asyncio
from temporalio import activity


# =============================================================================
# PAYMENT ACTIVITIES
# =============================================================================

@activity.defn
async def charge_payment(payment_method_id: str, amount: float, order_id: str) -> dict:
    """Charge payment - 10% failure rate for demo"""
    await asyncio.sleep(0.5)  # Simulate API call

    if random.random() < 0.1:
        raise Exception("Payment declined - insufficient funds")

    charge_id = f"CH-{uuid.uuid4().hex[:8]}"
    activity.logger.info(f"💳 Charged ${amount} for order {order_id} → {charge_id}")
    return {"charge_id": charge_id}


@activity.defn
async def refund_payment(charge_id: str) -> dict:
    """Refund payment - always succeeds (critical compensation)"""
    await asyncio.sleep(0.5)

    refund_id = f"RF-{uuid.uuid4().hex[:8]}"
    activity.logger.info(f"💰 Refunded payment {charge_id} → {refund_id}")
    return {"refund_id": refund_id}


# =============================================================================
# INVENTORY ACTIVITIES
# =============================================================================

@activity.defn
async def reserve_inventory(order_id: str, product_name: str) -> dict:
    """Reserve inventory"""
    await asyncio.sleep(0.3)

    reservation_id = f"RES-{uuid.uuid4().hex[:8]}"
    activity.logger.info(f"📦 Reserved inventory for {product_name} → {reservation_id}")
    return {"reservation_id": reservation_id}


@activity.defn
async def release_inventory(reservation_id: str) -> dict:
    """Release inventory - compensation"""
    await asyncio.sleep(0.3)

    activity.logger.info(f"📦 Released inventory reservation {reservation_id}")
    return {"released": True}


# =============================================================================
# FULFILLMENT ACTIVITIES
# =============================================================================

@activity.defn
async def create_fulfillment(order_id: str) -> dict:
    """Create fulfillment order"""
    await asyncio.sleep(0.5)

    fulfillment_id = f"FULL-{uuid.uuid4().hex[:8]}"
    activity.logger.info(f"Created fulfillment order -> {fulfillment_id}")
    return {"fulfillment_id": fulfillment_id}


@activity.defn
async def cancel_fulfillment(fulfillment_id: str) -> dict:
    """Cancel fulfillment - compensation"""
    await asyncio.sleep(0.3)

    activity.logger.info(f"🏭 Cancelled fulfillment {fulfillment_id}")
    return {"cancelled": True}


@activity.defn
async def request_pickup(fulfillment_id: str) -> dict:
    """Trigger delivery workflow via partner system (mock)"""
    await asyncio.sleep(0.3)

    pickup_request_id = f"PICKUP-{uuid.uuid4().hex[:8]}"
    activity.logger.info(f"Triggered delivery via partner system for {fulfillment_id} -> {pickup_request_id}")
    return {"pickup_request_id": pickup_request_id}


# =============================================================================
# NOTIFICATION ACTIVITIES
# =============================================================================

@activity.defn
async def send_notification(customer_email: str, subject: str, message: str) -> dict:
    """Send email notification - just logs"""
    activity.logger.info(f"📧 Email to {customer_email}: {subject}")
    activity.logger.info(f"   → {message}")
    return {"sent": True}
