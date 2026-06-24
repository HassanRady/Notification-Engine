import logging
from typing import Optional, Set, Dict
from app.domain.models import StatusEnum

logger = logging.getLogger(__name__)

class InvalidStateTransition(Exception):
    """Exception raised when a state transition violates validation rules or order sequence."""
    def __init__(self, current_status: Optional[StatusEnum], new_status: StatusEnum, msg: str):
        self.current_status = current_status
        self.new_status = new_status
        self.message = msg
        super().__init__(msg)

class DeliveryStateMachine:
    """
    Pure Python State Machine for validating and performing state transitions on delivery receipts.
    
    Allowed Transition Flow:
        None -> INGESTED -> SENT -> DELIVERED / FAILED
        
    Rules:
        - Monotonic sequence checks are enforced: any transition requires the new sequence_id to be strictly greater than the current sequence_id.
        - Regressions (e.g. SENT -> INGESTED, DELIVERED -> SENT) are strictly prohibited.
        - Terminal states (DELIVERED, FAILED) cannot transition to any other state.
    """
    _TRANSITION_MAP: Dict[Optional[StatusEnum], Set[StatusEnum]] = {
        None: {StatusEnum.INGESTED},
        StatusEnum.INGESTED: {StatusEnum.SENT},
        StatusEnum.SENT: {StatusEnum.DELIVERED, StatusEnum.FAILED},
        StatusEnum.DELIVERED: set(),
        StatusEnum.FAILED: set()
    }

    @classmethod
    def transition(
        cls,
        current_status: Optional[StatusEnum],
        current_sequence: Optional[int],
        new_status: StatusEnum,
        new_sequence: int
    ) -> StatusEnum:
        """
        Validates the proposed status transition and sequence monotonicity.
        
        Args:
            current_status: The current StatusEnum state, or None if no state is yet stored.
            current_sequence: The current integer sequence ID, or None if no state is yet stored.
            new_status: The proposed next StatusEnum state.
            new_sequence: The proposed next integer sequence ID.
            
        Returns:
            The validated new_status.
            
        Raises:
            InvalidStateTransition: If the transition violates flow constraints or sequence ordering.
        """
        # Ensure sequence ID is monotonically increasing
        if current_sequence is not None:
            if new_sequence <= current_sequence:
                msg = (
                    f"Invalid sequence progression: new sequence_id {new_sequence} must be "
                    f"strictly greater than current sequence_id {current_sequence}."
                )
                logger.warning(
                    f"Transition rejected due to sequence check: "
                    f"Current={current_status}(seq={current_sequence}) -> Proposed={new_status}(seq={new_sequence})"
                )
                raise InvalidStateTransition(current_status, new_status, msg)

        # Ensure state transition is valid
        allowed = cls._TRANSITION_MAP.get(current_status, set())
        if new_status not in allowed:
            msg = f"Invalid state transition: '{current_status}' -> '{new_status}' is not permitted."
            logger.warning(msg)
            raise InvalidStateTransition(current_status, new_status, msg)

        logger.info(
            f"State transitioned: {current_status} -> {new_status} (seq: {current_sequence} -> {new_sequence})"
        )
        return new_status
