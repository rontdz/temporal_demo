from datetime import timedelta
from typing import List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from models import OrderState, PreOrder, CompensationRecord


@workflow.defn
class PreOrderWorkflow:
    """
    Pre-order workflow demonstrating:
    - Durable timers (sleep until release date + 1 week)
    - Saga pattern (reverse compensation)
    - Human-in-the-loop (signals for fulfillment, item pick, delivery)
    """

    def __init__(self):
        self.state = OrderState.PRE_ORDER_PLACED
        self.order: Optional[PreOrder] = None

        # Saga compensation log
        self.compensation_log: List[CompensationRecord] = []

        # Resource IDs
        self.charge_id: Optional[str] = None
        self.reservation_id: Optional[str] = None
        self.fulfillment_id: Optional[str] = None

        # Signal flags
        self.cancel_requested = False
        self.start_fulfillment_requested = False
        self.item_picked_confirmed = False
        self.delivery_confirmed = False

    # =========================================================================
    # SIGNALS (4 total)
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

    # =========================================================================
    # MAIN WORKFLOW
    # =========================================================================

    @workflow.run
    async def run(self, order: PreOrder) -> dict:
        self.order = order
        workflow.logger.info(f"Starting pre-order workflow for {order.order_id}")

        # Phase 1: Initiate payment with payment gateway
        # If payment fails, no compensation needed (nothing charged yet)
        try:
            await self._process_payment()
        except Exception as e:
            workflow.logger.error(f"Payment failed: {e}")
            return {"status": "payment_failed", "order_id": order.order_id, "reason": str(e)}

        # Phase 2-3: Reserve inventory and wait for release date
        # If these fail, trigger compensation (refund payment)
        try:
            await self._reserve_inventory()
            await self._await_release_with_timeout()
        except Exception as e:
            workflow.logger.error(f"Failed during inventory/release phase: {e}")
            await self._compensate()
            return {"status": "refunded", "order_id": order.order_id, "reason": str(e)}

        # Phase 4-5: Fulfillment and delivery
        # These phases wait for signals - no failure/compensation path
        await self._process_fulfillment()
        await self._await_delivery_confirmation()

        # Order Completed!
        self._set_state(OrderState.DELIVERED)
        await self._notify("Order Completed!", f"Your {order.product_name} has been delivered!")

        return {"status": "completed", "order_id": order.order_id}

    # =========================================================================
    # PHASE 1: INITIATE PAYMENT WITH PAYMENT GATEWAY
    # =========================================================================

    async def _process_payment(self):
        """Initiate payment with payment gateway (automatic retry)"""
        self._set_state(OrderState.PAYMENT_PROCESSING)
        workflow.logger.info("Initiating payment with payment gateway...")

        result = await workflow.execute_activity(
            "charge_payment",
            args=[self.order.payment_method_id, self.order.amount, self.order.order_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
            ),
        )

        self.charge_id = result["charge_id"]
        self._record_compensation("payment_charged", self.charge_id)
        workflow.logger.info(f"Payment successful: {self.charge_id}")

        await self._notify(
            "Pre-Order Confirmed!",
            f"Payment of ${self.order.amount} received for {self.order.product_name}."
        )

    # =========================================================================
    # PHASE 2: RESERVE INVENTORY WITH INVENTORY SYSTEM
    # =========================================================================

    async def _reserve_inventory(self):
        """Reserve inventory with inventory system (retry, then saga if fails)"""
        workflow.logger.info("Reserving inventory with inventory system...")

        result = await workflow.execute_activity(
            "reserve_inventory",
            args=[self.order.order_id, self.order.product_name],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
            ),
        )

        self.reservation_id = result["reservation_id"]
        self._record_compensation("inventory_reserved", self.reservation_id)
        workflow.logger.info(f"Inventory reserved: {self.reservation_id}")

    # =========================================================================
    # PHASE 3: RELEASE DATE + 1 WEEK (DURABLE TIMER)
    # =========================================================================

    async def _await_release_with_timeout(self):
        """
        Wait until release date + 1 week deadline.
        - If fulfillment initiated → proceed to delivery
        - If not initiated by deadline → trigger saga (refund)
        """
        self._set_state(OrderState.AWAITING_RELEASE)

        # Calculate timeout: release_date + 1 week
        deadline = self.order.release_date + timedelta(weeks=1)
        now = workflow.now()
        wait_duration = deadline - now

        if wait_duration.total_seconds() <= 0:
            raise Exception("Release date + 1 week has passed - auto-refund triggered")

        workflow.logger.info(f"Waiting until release date + 1 week...")
        workflow.logger.info(f"   Release date: {self.order.release_date}")
        workflow.logger.info(f"   Deadline: {deadline}")
        workflow.logger.info(f"   Wait duration: {wait_duration.total_seconds():.0f}s")

        # Wait for fulfillment initiated signal OR timeout
        timed_out = False
        try:
            await workflow.wait_condition(
                lambda: self.start_fulfillment_requested or self.cancel_requested,
                timeout=wait_duration,
            )
        except:
            timed_out = True

        if self.cancel_requested:
            raise Exception("Order cancelled by customer")

        if timed_out:
            workflow.logger.warning("Not initiated by deadline - triggering refund")
            raise Exception("Fulfillment not initiated within deadline - auto-refund triggered")

        workflow.logger.info("Fulfillment initiated!")

    # =========================================================================
    # PHASE 4: TRIGGER DELIVERY WORKFLOW VIA PARTNER SYSTEM
    # =========================================================================

    async def _process_fulfillment(self):
        """Trigger delivery workflow via partner system"""
        self._set_state(OrderState.FULFILLMENT_IN_PROGRESS)
        workflow.logger.info("Initiating fulfillment...")

        # Create fulfillment order
        result = await workflow.execute_activity(
            "create_fulfillment",
            args=[self.order.order_id],
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.fulfillment_id = result["fulfillment_id"]
        self._record_compensation("fulfillment_created", self.fulfillment_id)

        # Trigger delivery workflow via partner system
        workflow.logger.info("Triggering delivery workflow via partner system...")
        await workflow.execute_activity(
            "request_pickup",
            args=[self.fulfillment_id],
            start_to_close_timeout=timedelta(seconds=30),
        )

        await self._notify(
            "Order Being Prepared",
            f"Your {self.order.product_name} is ready for pickup by delivery service."
        )

        # Wait for item_picked signal (with reminder notifications)
        await self._await_item_picked()

    async def _await_item_picked(self):
        """Wait for item to be picked, send reminders if not picked"""
        reminder_interval = timedelta(seconds=20)  # 20s for demo
        reminder_count = 0

        workflow.logger.info("Waiting for item to be picked...")

        while not self.item_picked_confirmed:
            try:
                await workflow.wait_condition(
                    lambda: self.item_picked_confirmed,
                    timeout=reminder_interval,
                )
            except:
                # Timeout - send pick up reminder
                reminder_count += 1
                workflow.logger.info(f"Sending pick up reminder #{reminder_count}")

                await workflow.execute_activity(
                    "send_notification",
                    args=["partner@example.com", f"Pick Up Reminder #{reminder_count}",
                          f"Order {self.order.order_id} is waiting to be picked up!"],
                    start_to_close_timeout=timedelta(seconds=10),
                )

        workflow.logger.info("Item picked!")

    # =========================================================================
    # PHASE 5: DELIVERY - WAIT FOR ITEM DELIVERED CONFIRMATION
    # =========================================================================

    async def _await_delivery_confirmation(self):
        """Wait for item to be delivered"""
        self._set_state(OrderState.AWAITING_DELIVERY)

        await self._notify(
            "Item Picked Up",
            f"Your {self.order.product_name} has been picked up and is on its way!"
        )

        workflow.logger.info("Waiting for item to be delivered...")

        await workflow.wait_condition(lambda: self.delivery_confirmed)

        workflow.logger.info("Item delivered!")

    # =========================================================================
    # SAGA COMPENSATION
    # =========================================================================

    def _record_compensation(self, action: str, resource_id: str):
        self.compensation_log.append(CompensationRecord(action=action, resource_id=resource_id))
        workflow.logger.info(f"Recorded for compensation: {action} -> {resource_id}")

    async def _compensate(self):
        """Execute compensation in REVERSE order (Saga pattern)"""
        self._set_state(OrderState.REFUNDED)

        workflow.logger.info(f"")
        workflow.logger.info(f"====== SAGA COMPENSATION ======")
        workflow.logger.info(f"   Compensating {len(self.compensation_log)} actions in REVERSE order:")

        for i, record in enumerate(reversed(self.compensation_log)):
            workflow.logger.info(f"   [{i+1}/{len(self.compensation_log)}] {record.action}")
            await self._compensate_action(record)

        workflow.logger.info(f"====== COMPENSATION COMPLETE ======")
        workflow.logger.info(f"")

        await self._notify(
            "Order Refunded",
            f"Your order has been cancelled and ${self.order.amount} will be refunded."
        )

    async def _compensate_action(self, record: CompensationRecord):
        compensation_map = {
            "payment_charged": "refund_payment",
            "inventory_reserved": "release_inventory",
            "fulfillment_created": "cancel_fulfillment",
        }

        activity_name = compensation_map.get(record.action)
        if not activity_name:
            return

        workflow.logger.info(f"       -> {activity_name}({record.resource_id})")

        await workflow.execute_activity(
            activity_name,
            args=[record.resource_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=100,  # Compensation MUST succeed
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=30),
            ),
        )

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _set_state(self, new_state: OrderState):
        workflow.logger.info(f"State: {self.state.value} -> {new_state.value}")
        self.state = new_state

    async def _notify(self, subject: str, message: str):
        await workflow.execute_activity(
            "send_notification",
            args=[self.order.customer_email, subject, message],
            start_to_close_timeout=timedelta(seconds=10),
        )
