import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from temporalio.client import Client

from models import PreOrder
from workflow import PreOrderWorkflow


async def place_order():
    """Place a new pre-order"""
    client = await Client.connect("localhost:7233")

    # Release date = now (in production, this would be months in the future)
    release_date = datetime.now(timezone.utc)

    order = PreOrder(
        order_id=f"ORD-{uuid.uuid4().hex[:8]}",
        customer_email="ron.teo@outlook.com",
        product_name="Mega Bot 2077",
        amount=888.00,
        payment_method_id="pm_card_visa",
        release_date=release_date,
    )

    handle = await client.start_workflow(
        PreOrderWorkflow.run,
        order,
        id=f"preorder-{order.order_id}",
        task_queue="preorder-queue",
    )

    print(f"✅ Order placed!")
    print(f"   Order ID: {order.order_id}")
    print(f"   Workflow ID: {handle.id}")
    print(f"   Amount: ${order.amount}")
    print(f"   Release date: {release_date}")
    print(f"   Deadline (release + 1 week): {release_date + timedelta(weeks=1)}")
    print(f"\n📺 Temporal UI: http://localhost:8080/namespaces/default/workflows/{handle.id}")
    print(f"\n⏳ Workflow will sleep until release date + 1 week deadline")
    print(f"   Send 'start-fulfillment' to proceed, or 'cancel' to trigger saga")

    return handle.id


# =============================================================================
# SIGNALS (4 total)
# =============================================================================

async def start_fulfillment(workflow_id: str):
    """Signal: Start the fulfillment process"""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)

    await handle.signal("start_fulfillment")
    print(f"✅ Signal 'start_fulfillment' sent")
    print(f"   Next: send 'item-picked' when warehouse picks the item")


async def cancel_order(workflow_id: str):
    """Signal: Cancel the order (triggers saga compensation)"""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)

    await handle.signal("cancel_order")
    print(f"✅ Signal 'cancel_order' sent")
    print(f"   SAGA compensation will run in reverse order!")


async def item_picked(workflow_id: str):
    """Signal: External delivery system picked up the item"""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)

    await handle.signal("item_picked")
    print(f"✅ Signal 'item_picked' sent")
    print(f"   Item is now on its way!")
    print(f"   Next: send 'confirm-delivery' when delivered")


async def confirm_delivery(workflow_id: str):
    """Signal: Confirm delivery complete"""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)

    await handle.signal("confirm_delivery")
    print(f"✅ Signal 'confirm_delivery' sent")
    print(f"   Order complete! 🎉")


# =============================================================================
# QUERIES
# =============================================================================

async def get_status(workflow_id: str):
    """Query: Get order status"""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)

    status = await handle.query("get_status")
    print(f"📋 Order Status:")
    print(f"   Order ID: {status['order_id']}")
    print(f"   State: {status['state']}")


async def get_compensation_log(workflow_id: str):
    """Query: Get compensation log (actions recorded for saga)"""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)

    log = await handle.query("get_compensation_log")
    print(f"📜 Compensation Log ({len(log)} actions recorded):")
    for i, record in enumerate(log, 1):
        print(f"   {i}. {record['action']} → {record['resource_id']}")


# =============================================================================
# CLI
# =============================================================================

def print_usage():
    print("Pre-Order Demo CLI")
    print("==================")
    print("")
    print("Start workflow:")
    print("  python client.py place-order")
    print("")
    print("Signals:")
    print("  python client.py start-fulfillment <workflow_id>")
    print("  python client.py cancel <workflow_id>")
    print("  python client.py item-picked <workflow_id>")
    print("  python client.py confirm-delivery <workflow_id>")
    print("")
    print("Queries:")
    print("  python client.py status <workflow_id>")
    print("  python client.py compensation-log <workflow_id>")


def main():
    import sys

    if len(sys.argv) < 2:
        print_usage()
        return

    command = sys.argv[1]

    # Place order
    if command == "place-order":
        asyncio.run(place_order())

    # Signals
    elif command == "start-fulfillment":
        if len(sys.argv) < 3:
            print("Error: workflow_id required")
            return
        asyncio.run(start_fulfillment(sys.argv[2]))

    elif command == "cancel":
        if len(sys.argv) < 3:
            print("Error: workflow_id required")
            return
        asyncio.run(cancel_order(sys.argv[2]))

    elif command == "item-picked":
        if len(sys.argv) < 3:
            print("Error: workflow_id required")
            return
        asyncio.run(item_picked(sys.argv[2]))

    elif command == "confirm-delivery":
        if len(sys.argv) < 3:
            print("Error: workflow_id required")
            return
        asyncio.run(confirm_delivery(sys.argv[2]))

    # Queries
    elif command == "status":
        if len(sys.argv) < 3:
            print("Error: workflow_id required")
            return
        asyncio.run(get_status(sys.argv[2]))

    elif command == "compensation-log":
        if len(sys.argv) < 3:
            print("Error: workflow_id required")
            return
        asyncio.run(get_compensation_log(sys.argv[2]))

    else:
        print(f"Unknown command: {command}")
        print_usage()


if __name__ == "__main__":
    main()
