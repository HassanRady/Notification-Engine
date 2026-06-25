import asyncio
import time
import logging
from typing import List, Dict, Any, Optional
from app.repository.notification import bulk_upsert_notification_states

logger = logging.getLogger(__name__)

class StatusMicroBatcher:
    """
    Micro-batcher that buffers incoming status receipts in an asyncio.Queue.
    
    A background worker processes the queue and performs high-performance bulk writes
    when either the batch size (1,000 items) is reached, or 100ms has elapsed.
    
    This pattern avoids busy-waiting and polling by suspending the worker task on 
    the queue until messages arrive.
    """
    def __init__(self, batch_size: int = 1000, flush_interval_ms: int = 100) -> None:
        self.batch_size = batch_size
        self.flush_interval = flush_interval_ms / 1000.0  # Convert to seconds
        self.queue: asyncio.Queue = asyncio.Queue()
        self.worker_task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        """Starts the background batching worker task."""
        if self._running:
            logger.warning("StatusMicroBatcher is already running.")
            return
        self._running = True
        self.worker_task = asyncio.create_task(self._batch_worker())
        logger.info("StatusMicroBatcher background worker started.")

    async def add(self, receipt: Dict[str, Any]) -> None:
        """
        Asynchronously adds a receipt status to the queue.
        This operation is thread/async-safe and non-blocking.
        """
        await self.queue.put(receipt)
        logger.debug(f"Queued status receipt for notification: {receipt.get('notification_id')}")

    async def _batch_worker(self) -> None:
        """
        Background worker that pools items from the queue and aggregates them in batches.
        Flushes when the batch is full or the timeout window has expired.
        """
        while self._running:
            batch: List[Dict[str, Any]] = []
            try:
                # Block until at least one item is available in the queue
                first_item = await self.queue.get()
                batch.append(first_item)
                self.queue.task_done()
                
                # Start timing the batch window from the receipt of the first item
                start_time = time.monotonic()
                
                while len(batch) < self.batch_size:
                    current_time = time.monotonic()
                    time_left = max(0.0, (start_time + self.flush_interval) - current_time)
                    
                    try:
                        # Wait for more items up to the remaining timeout duration
                        item = await asyncio.wait_for(self.queue.get(), timeout=time_left)
                        batch.append(item)
                        self.queue.task_done()
                    except asyncio.TimeoutError:
                        # Timeout reached: break out and flush what has been collected
                        break
                        
                # Flush the collected batch to the database
                await self._flush_batch(batch)
                
            except asyncio.CancelledError:
                # If cancelled, flush whatever we currently have in the batch
                if batch:
                    await self._flush_batch(batch)
                break
            except Exception as e:
                logger.error(f"Error in StatusMicroBatcher worker task: {e}", exc_info=True)
                # Prevent tight error loops
                await asyncio.sleep(0.1)

    async def _flush_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Invokes the database repository layer to commit the batch updates."""
        if not batch:
            return
        logger.info(f"Flushing micro-batch of {len(batch)} status updates to database.")
        try:
            await bulk_upsert_notification_states(batch)
        except Exception as e:
            logger.error(f"Failed to write micro-batch update block of size {len(batch)}: {e}", exc_info=True)

    async def stop(self) -> None:
        """Gracefully halts the background loop and flushes all remaining items in the queue."""
        logger.info("Stopping StatusMicroBatcher...")
        self._running = False
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
            finally:
                self.worker_task = None
        
        # Flush any remaining items left in the queue
        remaining_batch = []
        while not self.queue.empty():
            try:
                item = self.queue.get_nowait()
                remaining_batch.append(item)
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break
        
        if remaining_batch:
            await self._flush_batch(remaining_batch)
            
        logger.info("StatusMicroBatcher stopped successfully.")

# Global shared instance of the StatusMicroBatcher
status_batcher = StatusMicroBatcher()
