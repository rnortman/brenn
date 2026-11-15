"""The brenn bus attachment: attach to a brenn server's remote route, publish,
subscribe and read.

Every blocking call releases the GIL. On the main thread it runs the
interpreter's signal handlers at least every 100 ms, so Ctrl-C interrupts the
wait; `close` is not interrupted. Timeouts are seconds.
"""

import datetime
import os
from types import TracebackType
from typing import ClassVar, final

def attach(
    url: str,
    *,
    ident: str,
    timeout: float,
    token: str | None = None,
    token_file: str | os.PathLike[str] | None = None,
    ca_file: str | os.PathLike[str] | None = None,
) -> Client: ...
@final
class Client:
    def facts(self) -> AttachmentFacts: ...
    def status(self) -> Attached | Detached: ...
    def publish(
        self,
        channel: str,
        body: Body,
        *,
        timeout: float,
        urgency: Urgency = ...,
    ) -> PublishOutcome: ...
    def subscribe(
        self,
        channel: str,
        *,
        push_depth: int,
        retain_depth: int,
        timeout: float,
        cursorless: bool = False,
    ) -> Subscribed | Unavailable: ...
    def unsubscribe(self, channel: str) -> None: ...
    def recv(self, *, timeout: float | None) -> Event | None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Client: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...

@final
class AttachmentFacts:
    __match_args__ = ("protocol_version", "participant_id", "session_id", "max_body_bytes")
    @property
    def protocol_version(self) -> int: ...
    @property
    def participant_id(self) -> str: ...
    @property
    def session_id(self) -> str: ...
    @property
    def max_body_bytes(self) -> int: ...

@final
class Urgency:
    VERY_LOW: ClassVar[Urgency] = ...
    LOW: ClassVar[Urgency] = ...
    NORMAL: ClassVar[Urgency] = ...
    HIGH: ClassVar[Urgency] = ...

@final
class Body:
    def __new__(cls, data: bytes, content_type: str | None = None) -> Body: ...
    @staticmethod
    def text(text: str) -> Body: ...
    @property
    def data(self) -> bytes: ...
    @property
    def content_type(self) -> str | None: ...
    @property
    def is_text(self) -> bool: ...
    def as_text(self) -> str | None: ...

@final
class Gap:
    EPOCH_CHANGED: ClassVar[Gap] = ...
    BEYOND_RETAINED: ClassVar[Gap] = ...

@final
class LossKind:
    LIVENESS_TIMEOUT: ClassVar[LossKind] = ...
    TRANSPORT_CLOSED: ClassVar[LossKind] = ...

@final
class Loss:
    __match_args__ = ("kind", "code", "reason")
    @property
    def kind(self) -> LossKind: ...
    @property
    def code(self) -> int | None: ...
    @property
    def reason(self) -> str: ...

@final
class DialFailureKind:
    IO: ClassVar[DialFailureKind] = ...
    TLS: ClassVar[DialFailureKind] = ...
    UPGRADE_REJECTED: ClassVar[DialFailureKind] = ...
    HANDSHAKE: ClassVar[DialFailureKind] = ...
    TIMED_OUT: ClassVar[DialFailureKind] = ...

@final
class DialFailure:
    __match_args__ = ("kind", "status", "detail")
    @property
    def kind(self) -> DialFailureKind: ...
    @property
    def status(self) -> int | None: ...
    @property
    def detail(self) -> str | None: ...

class PublishOutcome: ...

@final
class Ok(PublishOutcome): ...

@final
class Failed(PublishOutcome): ...

@final
class RateLimited(PublishOutcome): ...

@final
class BodyTooLarge(PublishOutcome):
    __match_args__ = ("len", "max")
    @property
    def len(self) -> int: ...
    @property
    def max(self) -> int: ...

@final
class TextOnlyChannel(PublishOutcome): ...

@final
class Subscribed:
    __match_args__ = ("replay_count", "gap")
    @property
    def replay_count(self) -> int: ...
    @property
    def gap(self) -> Gap | None: ...

@final
class Unavailable: ...

class Event: ...

@final
class Delivery(Event):
    __match_args__ = (
        "channel",
        "message_id",
        "sender",
        "publish_ts",
        "reply_to",
        "urgency",
        "body",
        "seq",
        "dropped",
    )
    @property
    def channel(self) -> str: ...
    @property
    def message_id(self) -> str: ...
    @property
    def sender(self) -> str: ...
    @property
    def publish_ts(self) -> datetime.datetime: ...
    @property
    def reply_to(self) -> str | None: ...
    @property
    def urgency(self) -> Urgency: ...
    @property
    def body(self) -> Body: ...
    @property
    def seq(self) -> int: ...
    @property
    def dropped(self) -> int: ...

@final
class Resubscribed(Event):
    __match_args__ = ("channel", "replay_count", "gap")
    @property
    def channel(self) -> str: ...
    @property
    def replay_count(self) -> int: ...
    @property
    def gap(self) -> Gap | None: ...

@final
class SubscriptionDropped(Event):
    __match_args__ = ("channel",)
    @property
    def channel(self) -> str: ...

@final
class Attached(Event):
    __match_args__ = ("facts",)
    @property
    def facts(self) -> AttachmentFacts: ...

@final
class Detached(Event):
    __match_args__ = ("loss", "last_dial_failure")
    @property
    def loss(self) -> Loss: ...
    @property
    def last_dial_failure(self) -> DialFailure | None: ...

class BusError(Exception): ...

class AttachTimeout(BusError):
    last_failure: DialFailure | None

class NotAttached(BusError): ...

class ConnectionLost(BusError):
    loss: Loss

class AnswerTimeout(BusError): ...
class AttachmentEnded(BusError): ...

class CredentialRejected(AttachmentEnded):
    status: int

class IncompatibleVersions(AttachmentEnded):
    ours: tuple[int, int]
    theirs: tuple[int, int]

class ProtocolError(AttachmentEnded):
    detail: str

class Futile(AttachmentEnded):
    attachments: int

class ClientClosed(AttachmentEnded): ...

class SupervisorPanicked(AttachmentEnded):
    message: str

class AlreadySubscribed(BusError, ValueError): ...
class NotSubscribed(BusError, ValueError): ...
