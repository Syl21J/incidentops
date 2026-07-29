"""Validated order event model and deterministic event generation."""

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

Currency = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]
SchemaVersion = Annotated[str, StringConstraints(pattern=r"^[1-9]\d*\.\d+$")]

DEFAULT_GENERATION_TIME = datetime(2026, 1, 1, tzinfo=UTC)
CURRENCIES = ("EUR", "USD", "GBP")


class OrderEvent(BaseModel):
    """Versioned order event exchanged as UTF-8 JSON through Kafka."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    order_id: UUID
    customer_id: str = Field(min_length=1, max_length=128)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    created_at: AwareDatetime
    schema_version: SchemaVersion = "1.0"

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        """Normalize every accepted timestamp to UTC."""

        return value.astimezone(UTC)

    def to_json_bytes(self) -> bytes:
        """Serialize the event as UTF-8 JSON."""

        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> "OrderEvent":
        """Validate an event from UTF-8 JSON bytes."""

        return cls.model_validate_json(payload)


def generate_order_event(
    *,
    index: int,
    seed: int,
    run_id: str,
    base_time: datetime = DEFAULT_GENERATION_TIME,
    schema_version: str = "1.0",
) -> OrderEvent:
    """Generate a deterministic valid event for a seed, run identifier, and index."""

    if index < 0:
        raise ValueError("index must not be negative")

    random_generator = random.Random(f"{seed}:{run_id}:{index}")
    amount = Decimal(random_generator.randint(100, 100_000)) / Decimal(100)
    customer_number = random_generator.randint(1, 1_000)

    return OrderEvent(
        event_id=uuid5(NAMESPACE_URL, f"incidentops:event:{run_id}:{index}"),
        order_id=uuid5(NAMESPACE_URL, f"incidentops:order:{run_id}:{index}"),
        customer_id=f"{run_id}-customer-{customer_number:04d}",
        amount=amount,
        currency=random_generator.choice(CURRENCIES),
        created_at=base_time.astimezone(UTC) + timedelta(milliseconds=index),
        schema_version=schema_version,
    )
