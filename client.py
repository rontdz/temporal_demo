import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from temporalio.client import Client

from models import PreOrder
from workflow import PreOrderWorkflow


async def get_workflow_handle(workflow_id: str):
    """Helper to get a workflow handle"""
    client = await Client.connect("localhost:7233")
    return client.get_workflow_handle(workflow_id)


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
    handle = await get_workflow_handle(workflow_id)
    await handle.signal("start_fulfillment")
    print(f"✅ Fulfillment initiated. You will receive a notification when processed.")
    print(f"   Next: Run 'item-picked' when the item has been picked from warehouse.")


async def cancel_order(workflow_id: str):
    """Signal: Cancel the order (triggers saga compensation)"""
    handle = await get_workflow_handle(workflow_id)
    await handle.signal("cancel_order")
    print(f"✅ Cancellation initiated. Compensation will run in reverse order.")
    print(f"   You will receive a refund notification when processed.")


async def item_picked(workflow_id: str):
    """Signal: External delivery system picked up the item"""
    handle = await get_workflow_handle(workflow_id)
    await handle.signal("item_picked")
    print(f"✅ Item pickup confirmed. Delivery is now in progress.")
    print(f"   Next: Run 'confirm-delivery' when the item has been delivered.")


async def confirm_delivery(workflow_id: str):
    """Signal: Confirm delivery complete"""
    handle = await get_workflow_handle(workflow_id)
    await handle.signal("confirm_delivery")
    print(f"✅ Delivery confirmed. Order will be marked as completed.")
    print(f"   You will receive a completion notification when processed.")


# =============================================================================
# QUERIES
# =============================================================================

async def get_status(workflow_id: str):
    """Query: Get order status"""
    handle = await get_workflow_handle(workflow_id)
    status = await handle.query("get_status")
    print(f"📋 Order Status:")
    print(f"   Order ID: {status['order_id']}")
    print(f"   State: {status['state']}")


async def get_compensation_log(workflow_id: str):
    """Query: Get compensation log (actions recorded for saga)"""
    handle = await get_workflow_handle(workflow_id)
    log = await handle.query("get_compensation_log")
    print(f"📜 Compensation Log ({len(log)} actions recorded):")
    for i, record in enumerate(log, 1):
        print(f"   {i}. {record['action']} → {record['resource_id']}")


async def get_deadline_info(workflow_id: str):
    """Query: Get deadline and remaining time"""
    handle = await get_workflow_handle(workflow_id)
    info = await handle.query("get_deadline_info")

    if not info["deadline"]:
        print("⏰ No deadline set (workflow not in waiting phase)")
        return

    # Calculate remaining time on client side (using real current time)
    deadline_dt = datetime.fromisoformat(info["deadline"])
    now = datetime.now(timezone.utc)
    remaining = (deadline_dt - now).total_seconds()
    remaining = max(0, remaining)

    days = int(remaining // 86400)
    hours = int((remaining % 86400) // 3600)
    minutes = int((remaining % 3600) // 60)
    seconds = int(remaining % 60)

    print(f"⏰ Deadline Info:")
    print(f"   Deadline: {info['deadline']}")
    print(f"   Remaining: {days}d {hours}h {minutes}m {seconds}s")


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
    print("  python client.py deadline <workflow_id>")
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

    elif command == "deadline":
        if len(sys.argv) < 3:
            print("Error: workflow_id required")
            return
        asyncio.run(get_deadline_info(sys.argv[2]))

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
