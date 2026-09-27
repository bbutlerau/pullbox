"""Verification is explicit, while unresolved conflicts and rejections persist."""

from typing import cast

import pytest

from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
    transition_identity_state,
)
from pullbox.core.metadata_identity_state import (
    IdentityVerificationAction as Action,
)
from pullbox.core.metadata_identity_state import (
    IdentityVerificationState as State,
)


@pytest.mark.parametrize("state", list(State))
def test_observation_alone_never_changes_verification_or_rejection(state: State) -> None:
    assert transition_identity_state(state, Action.OBSERVE) is state


@pytest.mark.parametrize("state", [State.OBSERVED, State.VERIFIED, State.STALE])
def test_validated_verification_can_establish_or_refresh_identity(state: State) -> None:
    assert transition_identity_state(state, Action.VERIFY) is State.VERIFIED


@pytest.mark.parametrize("state", [State.CONFLICTED, State.REJECTED])
def test_automatic_verification_cannot_clear_conflict_or_user_rejection(state: State) -> None:
    with pytest.raises(IdentityReviewRequiredError):
        transition_identity_state(state, Action.VERIFY)


@pytest.mark.parametrize("state", list(State))
def test_conflict_does_not_override_an_existing_rejection(state: State) -> None:
    expected = State.REJECTED if state is State.REJECTED else State.CONFLICTED
    assert transition_identity_state(state, Action.REPORT_CONFLICT) is expected


@pytest.mark.parametrize("state", list(State))
def test_provider_missing_or_expiry_does_not_clear_review_state(state: State) -> None:
    expected = state if state in (State.REJECTED, State.CONFLICTED) else State.STALE
    assert transition_identity_state(state, Action.MARK_STALE) is expected


@pytest.mark.parametrize("state", list(State))
def test_explicit_rejection_is_durable(state: State) -> None:
    assert transition_identity_state(state, Action.REJECT) is State.REJECTED


@pytest.mark.parametrize("state", list(State))
def test_explicit_review_confirmation_can_resolve_a_proposal(state: State) -> None:
    assert transition_identity_state(state, Action.CONFIRM) is State.VERIFIED


@pytest.mark.parametrize("invalid", [None, "verified", "not-a-state", 1, True])
def test_unknown_states_are_not_treated_as_verified(invalid: object) -> None:
    with pytest.raises(ValueError, match="state"):
        transition_identity_state(cast("State", invalid), Action.CONFIRM)


@pytest.mark.parametrize("invalid", [None, "verify", "not-an-action", 1, True])
def test_unvalidated_action_strings_are_not_accepted(invalid: object) -> None:
    with pytest.raises(ValueError, match="action"):
        transition_identity_state(State.OBSERVED, cast("Action", invalid))


def test_rescan_and_provider_refresh_cannot_undo_a_user_rejection() -> None:
    state = transition_identity_state(State.OBSERVED, Action.REJECT)
    for action in (Action.OBSERVE, Action.REPORT_CONFLICT, Action.MARK_STALE):
        state = transition_identity_state(state, action)
        assert state is State.REJECTED
    with pytest.raises(IdentityReviewRequiredError):
        transition_identity_state(state, Action.VERIFY)
    assert transition_identity_state(state, Action.CONFIRM) is State.VERIFIED
