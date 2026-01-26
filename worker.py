import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from workflow import PreOrderWorkflow
from activities import (
    charge_payment,
    refund_payment,
    reserve_inventory,
    release_inventory,
    create_fulfillment,
    cancel_fulfillment,
    request_pickup,
    send_notification,
)


async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Create worker
    worker = Worker(
        client,
        task_queue="preorder-queue",
        workflows=[PreOrderWorkflow],
        activities=[
            charge_payment,
            refund_payment,
            reserve_inventory,
            release_inventory,
            create_fulfillment,
            cancel_fulfillment,
            request_pickup,
            send_notification,
        ],
    )

    print("⚡ Worker started! Listening on 'preorder-queue'...")
    print("   Press Ctrl+C to stop\n")

    # Run the worker
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())