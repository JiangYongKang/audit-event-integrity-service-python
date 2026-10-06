import pytest

from app.service import AuditService


def make_event(event_id, sequence, source="s1", timestamp=1700000000000,
               event_type="login", payload=None):
    return {
        "event_id": event_id,
        "source": source,
        "sequence": sequence,
        "timestamp": timestamp,
        "event_type": event_type,
        "payload": payload or {},
    }


@pytest.fixture
def service():
    return AuditService()


@pytest.fixture
def event_factory():
    return make_event
