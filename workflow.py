from datetime import datetime, timedelta
from typing import List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from models import OrderState, PreOrder, CompensationRecord


@workflow.defn
class PreOrderWorkflow:
    """
    E-commerce pre-order workflow that handles the complete lifecycle from
    payment to delivery, spanning months between order placement and fulfillment.

    The workflow coordinates payment gateway, inventory, fulfillment, and delivery
    systems. It waits for the product release date plus a 1-week grace period for
    fulfillment to begin. If fulfillment isn't initiated by the deadline, the order
    is automatically cancelled and refunded.
    """

    def __init__(self):
        self.state = OrderState.PRE_ORDER_PLACED
        self.order: Optional[PreOrder] = None
        self.compensation_log: List[CompensationRecord] = []

        # Signal flags
        self.cancel_requested = False
        self.start_fulfillment_requested = False
        self.item_picked_confirmed = False
        self.delivery_confirmed = False

        # Deadline tracking
        self.deadline: Optional[datetime] = None

    # =========================================================================
    # MAIN WORKFLOW
    # =========================================================================

    @workflow.run
    async def run(self, order: PreOrder) -> dict:
        self.order = order
        workflow.logger.info(f"Starting pre-order workflow for {order.order_id}")

        # =====================================================================
        # PHASE 1: PAYMENT PROCESSING
        # =====================================================================
        self._set_state(OrderState.PAYMENT_PROCESSING)

        try:
            result = await workflow.execute_activity(
                "charge_payment",
                args=[order.payment_method_id, order.amount, order.order_id],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=2)),
            )
            charge_id = result["charge_id"]
            self._record_compensation("payment_charged", charge_id)

            await self._notify(
                "Pre-Order Confirmed!",
                f"Payment of ${order.amount} received for {order.product_name}."
            )
        except Exception as e:
            workflow.logger.error(f"Payment failed: {e}")
            return {"status": "payment_failed", "order_id": order.order_id, "reason": str(e)}

        # =====================================================================
        # PHASE 2: RESERVE INVENTORY
        # =====================================================================
        try:
            result = await workflow.execute_activity(
                "reserve_inventory",
                args=[order.order_id, order.product_name],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=2)),
            )
            reservation_id = result["reservation_id"]
            self._record_compensation("inventory_reserved", reservation_id)

        except Exception as e:
            workflow.logger.error(f"Inventory reservation failed: {e}")
            await self._compensate()
            return {"status": "refunded", "order_id": order.order_id, "reason": str(e)}

        # =====================================================================
        # PHASE 3: PENDING UNTIL RELEASE DATE + 1 WEEK
        # =====================================================================
        self._set_state(OrderState.AWAITING_RELEASE)

        # Deadline is release date + 1 week buffer
        self.deadline = order.release_date + timedelta(weeks=1)

        # Use workflow.now instead to ensure deterministic
        wait_duration = self.deadline - workflow.now()

        if wait_duration.total_seconds() <= 0:
            await self._compensate()
            return {"status": "refunded", "order_id": order.order_id, "reason": "Release date + 1 week has passed"}

        workflow.logger.info(f"Waiting until {self.deadline} (release date + 1 week)...")

        # Wait for fulfillment signal OR cancel signal OR timeout
        timed_out = False
        try:
            await workflow.wait_condition(
                lambda: self.start_fulfillment_requested or self.cancel_requested,
                timeout=wait_duration,
            )
        except:
            timed_out = True

        if self.cancel_requested:
            await self._compensate()
            return {"status": "refunded", "order_id": order.order_id, "reason": "Order cancelled by customer"}

        if timed_out:
            await self._compensate()
            return {"status": "refunded", "order_id": order.order_id, "reason": "Fulfillment not initiated by deadline"}

        # =====================================================================
        # PHASE 4: TRIGGER DELIVERY WORKFLOW
        # =====================================================================
        self._set_state(OrderState.FULFILLMENT_IN_PROGRESS)

        # Start fulfillment process
        result = await workflow.execute_activity(
            "create_fulfillment",
            args=[order.order_id],
            start_to_close_timeout=timedelta(seconds=30),
        )
        fulfillment_id = result["fulfillment_id"]
        self._record_compensation("fulfillment_created", fulfillment_id)

        # Initiate the pick up process
        await workflow.execute_activity(
            "request_pickup",
            args=[fulfillment_id],
            start_to_close_timeout=timedelta(seconds=30),
        )

        await self._notify(
            "Order Being Prepared",
            f"Your {order.product_name} is ready for pickup by delivery service."
        )

        # Wait for item_picked signal (with reminder notifications)
        reminder_interval = timedelta(seconds=20)
        reminder_count = 0

        # Continue send reminder every 20 seconds until pick up signal
        while not self.item_picked_confirmed:
            try:
                await workflow.wait_condition(lambda: self.item_picked_confirmed, timeout=reminder_interval)
            except:
                reminder_count += 1
                await workflow.execute_activity(
                    "send_notification",
                    args=["partner@example.com", f"Pick Up Reminder #{reminder_count}",
                          f"Order {order.order_id} is waiting to be picked up!"],
                    start_to_close_timeout=timedelta(seconds=10),
                )

        # =====================================================================
        # PHASE 5: DELIVERY
        # =====================================================================
        self._set_state(OrderState.AWAITING_DELIVERY)

        await self._notify(
            "Item Picked Up",
            f"Your {order.product_name} has been picked up and is on its way!"
        )

        await workflow.wait_condition(lambda: self.delivery_confirmed)

        # =====================================================================
        # ORDER COMPLETED
        # =====================================================================
        self._set_state(OrderState.DELIVERED)
        await self._notify("Order Completed!", f"Your {order.product_name} has been delivered!")

        return {"status": "completed", "order_id": order.order_id}

    # =========================================================================
    # SIGNALS
    # =========================================================================

    @workflow.signal
    def start_fulfillment(self):
        """Signal to begin fulfillment process"""
        workflow.logger.info("Signal: start_fulfillment")
        self.start_fulfillment_requested = True

    @workflow.signal
    def cancel_order(self):
        """Signal to cancel order (triggers saga)"""
        workflow.logger.info("Signal: cancel_order")
        self.cancel_requested = True

    @workflow.signal
    def item_picked(self):
        """Signal: external delivery system picked up the item"""
        workflow.logger.info("Signal: item_picked")
        self.item_picked_confirmed = True

    @workflow.signal
    def confirm_delivery(self):
        """Signal to confirm delivery complete"""
        workflow.logger.info("Signal: confirm_delivery")
        self.delivery_confirmed = True

    # =========================================================================
    # QUERIES
    # =========================================================================

    @workflow.query
    def get_status(self) -> dict:
        return {
            "order_id": self.order.order_id if self.order else None,
            "state": self.state.value,
        }

    @workflow.query
    def get_compensation_log(self) -> List[dict]:
        return [{"action": r.action, "resource_id": r.resource_id} for r in self.compensation_log]

    @workflow.query
    def get_deadline_info(self) -> dict:
        """Returns the deadline (client calculates remaining time)"""
        return {
            "deadline": self.deadline.isoformat() if self.deadline else None,
        }

    # =========================================================================
    # HELPERS
    # =========================================================================

    # State management
    def _set_state(self, new_state: OrderState):
        workflow.logger.info(f"State: {self.state.value} -> {new_state.value}")
        self.state = new_state

    # Record compensation actions in order
    def _record_compensation(self, action: str, resource_id: str):
        self.compensation_log.append(CompensationRecord(action=action, resource_id=resource_id))

    # Send notification (helper)
    async def _notify(self, subject: str, message: str):
        await workflow.execute_activity(
            "send_notification",
            args=[self.order.customer_email, subject, message],
            start_to_close_timeout=timedelta(seconds=10),
        )

    # Saga compensation (reverse order)
    async def _compensate(self):
        """Execute compensation in REVERSE order (Saga pattern)"""
        self._set_state(OrderState.REFUNDED)
        workflow.logger.info(f"====== SAGA COMPENSATION ({len(self.compensation_log)} actions) ======")

        # Map actions to their compensation activities
        compensation_map = {
            "payment_charged": "refund_payment",
            "inventory_reserved": "release_inventory",
            "fulfillment_created": "cancel_fulfillment",
        }

        # Execute compensation in reverse order
        for record in reversed(self.compensation_log):
            activity_name = compensation_map.get(record.action)
            if activity_name:
                workflow.logger.info(f"   {record.action} -> {activity_name}({record.resource_id})")
                await workflow.execute_activity(
                    activity_name,
                    args=[record.resource_id],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        maximum_attempts=100,
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=30),
                    ),
                )

        await self._notify(
            "Order Refunded",
            f"Your order has been cancelled and ${self.order.amount} will be refunded."
        )
