"""Configuration for the OpenObserve client.

This file is owned by the service package (generated once by sdk-kit).
"""

from __future__ import annotations

import base64
import dataclasses
import re
from collections.abc import Mapping
from typing import Any, ClassVar, TypeVar

from ._core import BaseConfig, ConfigError, EnvReader, Secret

__all__ = [
    "DEFAULT_ORG",
    "DEFAULT_STREAM",
    "DEFAULT_URL",
    "OpenObserveConfig",
    "normalize_stream_name",
]

#: Default endpoint when neither the config nor the environment sets one.
DEFAULT_URL = "http://127.0.0.1:5080"
#: Default organization identifier.
DEFAULT_ORG = "default"
#: Default log and traces stream (the server's OTLP default).
DEFAULT_STREAM = "default"

_C = TypeVar("_C", bound="OpenObserveConfig")
_NON_STREAM = re.compile(r"[^a-z0-9_]")
_BAD_ORG = re.compile(r"[/?#\s]")
_PRE_ENCODED = re.compile(r"^basic\s", re.IGNORECASE)


def normalize_stream_name(name: str) -> str:
    """Server-side stream name normalization: lower case, every non ``[a-z0-9_]`` → ``_``.

    >>> normalize_stream_name("Sdk-Mixed.x")
    'sdk_mixed_x'
    """
    return _NON_STREAM.sub("_", name.strip().lower())


def _is_pre_encoded(token: str) -> bool:
    return bool(_PRE_ENCODED.match(token.strip()))


def _basic(user: str, secret: str) -> str:
    raw = f"{user}:{secret}".encode()
    return "Basic " + base64.b64encode(raw).decode("ascii")


@dataclasses.dataclass(frozen=True, repr=False)
class OpenObserveConfig(BaseConfig):
    """Immutable settings for :class:`~grow_openobserve.OpenObserveClient`.

    Common fields (``url``, timeouts, ``retry``, ``tls``, ``proxy``, ``headers``,
    ``user_agent_suffix``, ``hooks``, ``logger``) come from :class:`BaseConfig`. Durations are
    float seconds; ``datetime.timedelta`` is accepted.

    OpenObserve only accepts HTTP Basic auth: ``user`` + ``password`` (a root or normal user)
    or ``user`` + ``token`` (a service-account e-mail and its token). A ``token`` that starts
    with ``Basic `` is a pre-encoded header and is sent verbatim (no ``user`` needed).
    ``password`` and ``token`` accept a ``str`` and are stored as :class:`Secret`; ``repr()``
    never shows them.

    Environment (``from_env``): ``GROW_OPENOBSERVE_<NAME>`` first, then ``OPENOBSERVE_<NAME>``
    for ``URL``, ``ORG``, ``USER``, ``PASSWORD``, ``TOKEN``, ``STREAM``, ``TRACES_STREAM``;
    ``GROW_OPENOBSERVE_`` only for ``CONNECT_TIMEOUT``, ``REQUEST_TIMEOUT``, ``TOTAL_TIMEOUT``,
    ``MAX_RETRIES``, ``CA_CERT``, ``CLIENT_CERT``, ``CLIENT_KEY``, ``INSECURE_SKIP_VERIFY``,
    ``PROXY``.
    """

    #: Vendor variables per GROW_* suffix, in precedence order.
    ENV_VENDOR: ClassVar[Mapping[str, tuple[str, ...]]] = {
        "URL": ("OPENOBSERVE_URL",),
        "ORG": ("OPENOBSERVE_ORG",),
        "USER": ("OPENOBSERVE_USER",),
        "PASSWORD": ("OPENOBSERVE_PASSWORD",),
        "TOKEN": ("OPENOBSERVE_TOKEN",),
        "STREAM": ("OPENOBSERVE_STREAM",),
        "TRACES_STREAM": ("OPENOBSERVE_TRACES_STREAM",),
    }

    url: str = DEFAULT_URL
    #: Organization identifier (``/api/{org}/…``).
    org: str = DEFAULT_ORG
    #: Basic-auth user: a login e-mail or a service-account e-mail.
    user: str | None = None
    #: Password of ``user``; mutually exclusive with ``token``.
    password: Secret | None = None
    #: Service-account token (or a pre-encoded ``Basic …`` header).
    token: Secret | None = None
    #: Default log stream (normalized like the server does).
    stream: str = DEFAULT_STREAM
    #: Default traces stream (normalized like the server does).
    traces_stream: str = DEFAULT_STREAM

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "org", (self.org or "").strip())
        user = self.user.strip() if self.user else None
        object.__setattr__(self, "user", user or None)
        object.__setattr__(self, "stream", normalize_stream_name(self.stream or ""))
        object.__setattr__(self, "traces_stream", normalize_stream_name(self.traces_stream or ""))

    @classmethod
    def _env_values(cls, reader: EnvReader) -> dict[str, Any]:
        values = super()._env_values(reader)
        vendor = cls.ENV_VENDOR
        for field, suffix in (
            ("org", "ORG"),
            ("user", "USER"),
            ("stream", "STREAM"),
            ("traces_stream", "TRACES_STREAM"),
        ):
            text = reader.string(suffix, vendor.get(suffix, ()))
            if text is not None:
                values[field] = text
        for field, suffix in (("password", "PASSWORD"), ("token", "TOKEN")):
            text = reader.string(suffix, vendor.get(suffix, ()))
            if text is not None:
                values[field] = Secret(text)
        return values

    @classmethod
    def from_env(cls: type[_C], env: Mapping[str, str] | None = None, **overrides: Any) -> _C:
        """Build from the environment, then validate.

        Precedence: explicit keyword > ``GROW_OPENOBSERVE_<NAME>`` > ``OPENOBSERVE_<NAME>`` >
        default. An explicit ``password`` drops a ``token`` found in the environment and vice
        versa, so overriding the credential kind never trips the "password and token" rule.
        ``env`` defaults to ``os.environ``; pass a dict in tests.
        """
        values = cls._env_values(EnvReader(env))
        if overrides.get("password") is not None:
            values.pop("token", None)
        if overrides.get("token") is not None:
            values.pop("password", None)
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values).validate()

    def validate(self: _C) -> _C:
        """Common checks plus the OpenObserve rules; raises :class:`ConfigError`."""
        super().validate()
        if not self.org or _BAD_ORG.search(self.org):
            raise ConfigError("config: org must be a non-empty identifier")
        if not self.stream:
            raise ConfigError("config: stream must not be blank")
        if not self.traces_stream:
            raise ConfigError("config: traces_stream must not be blank")
        if self.password is not None and self.token is not None:
            raise ConfigError("config: set either password or token, not both")
        if (self.password is not None and self.password.reveal() == "") or (
            self.token is not None and not self.token.reveal().strip()
        ):
            raise ConfigError("config: credentials must not be blank")
        pre_encoded = self.token is not None and _is_pre_encoded(self.token.reveal())
        if (self.password is not None or (self.token is not None and not pre_encoded)) and (
            not self.user
        ):
            raise ConfigError("config: user is required with password or token")
        if self.user and self.password is None and self.token is None:
            raise ConfigError("config: user requires password or token")
        return self

    @property
    def has_credentials(self) -> bool:
        """Whether a password or token is configured."""
        return self.password is not None or self.token is not None

    def authorization(self) -> str | None:
        """The ``Authorization`` header value (``Basic …``) or ``None`` without credentials.

        The result is a credential: never log it.
        """
        if self.token is not None:
            token = self.token.reveal().strip()
            if _is_pre_encoded(token):
                return "Basic " + token[6:].strip()
            return _basic(self.user or "", token)
        if self.password is not None:
            return _basic(self.user or "", self.password.reveal())
        return None

    def known_secrets(self) -> list[str]:
        """Credential values (and the encoded Basic header) to redact from errors and logs."""
        out = super().known_secrets()
        header = self.authorization()
        if header is not None:
            out.append(header[6:])
        return out
