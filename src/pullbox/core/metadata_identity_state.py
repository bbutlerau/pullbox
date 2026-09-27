"""Identity evidence lifecycle; state changes do not authorize attachment."""

import enum
from typing import assert_never


class IdentityVerificationState(enum.StrEnum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    CONFLICTED = "conflicted"
    STALE = "stale"
    REJECTED = "rejected"


class IdentityVerificationAction(enum.StrEnum):
    OBSERVE = "observe"
    VERIFY = "verify"
    REPORT_CONFLICT = "report_conflict"
    MARK_STALE = "mark_stale"
    REJECT = "reject"
    CONFIRM = "confirm"


class IdentityReviewRequiredError(ValueError):
    """An automatic verification cannot replace an unresolved human decision."""


def transition_identity_state(
    state: IdentityVerificationState, action: IdentityVerificationAction
) -> IdentityVerificationState:
    """Apply a validated lifecycle action without database or provider work.

    Callers must validate evidence before VERIFY and explicit user intent before
    CONFIRM or REJECT. Enum values are not authorization tokens. A verified state
    does not prove cross-provider agreement, parent membership, or uniqueness.
    Those checks and any attachment/history writes belong in one transaction.
    """
    if not isinstance(state, IdentityVerificationState):
        raise ValueError("Unknown identity verification state")
    if not isinstance(action, IdentityVerificationAction):
        raise ValueError("Unknown identity verification action")
    match action:
        case IdentityVerificationAction.OBSERVE:
            return state
        case IdentityVerificationAction.VERIFY:
            if state in (IdentityVerificationState.CONFLICTED, IdentityVerificationState.REJECTED):
                raise IdentityReviewRequiredError("This identity requires explicit review")
            return IdentityVerificationState.VERIFIED
        case IdentityVerificationAction.CONFIRM:
            return IdentityVerificationState.VERIFIED
        case IdentityVerificationAction.REJECT:
            return IdentityVerificationState.REJECTED
        case IdentityVerificationAction.REPORT_CONFLICT:
            if state is IdentityVerificationState.REJECTED:
                return state
            return IdentityVerificationState.CONFLICTED
        case IdentityVerificationAction.MARK_STALE:
            if state in (IdentityVerificationState.REJECTED, IdentityVerificationState.CONFLICTED):
                return state
            return IdentityVerificationState.STALE
        case _:
            assert_never(action)
