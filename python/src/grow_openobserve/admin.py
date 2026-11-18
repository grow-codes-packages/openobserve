"""Admin helpers: organizations, service accounts and the ingest passcode (``client.admin``).

Tokens and passcodes returned by the server are wrapped in :class:`Secret` and added to the
client's redaction list, so they never show up in later error messages.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, TypeVar

from ._core import CancelToken, Secret, ValidationError
from ._http import (
    Call,
    Ctx,
    as_dict,
    as_int,
    as_list,
    as_str,
    json_body,
    micros_to_dt,
    org_path,
    request,
    run_async,
    run_sync,
)

if TYPE_CHECKING:
    from .config import OpenObserveConfig

__all__ = [
    "AsyncOpenObserveAdmin",
    "OpenObserveAdmin",
    "Organization",
    "Passcode",
    "ServiceAccount",
    "ServiceAccountToken",
]

_T = TypeVar("_T")
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+$")


@dataclass(frozen=True)
class Organization:
    """An OpenObserve organization (``type`` is ``default`` or ``custom``)."""

    id: int
    identifier: str
    name: str
    type: str
    user_email: str = ""


@dataclass(frozen=True)
class ServiceAccountToken:
    """A service account with its usable token.

    Only ``create_service_account`` and ``rotate_service_account_token`` return the token; use
    it as ``OpenObserveConfig(user=email, token=token)``.
    """

    email: str
    token: Secret


@dataclass(frozen=True)
class ServiceAccount:
    """A listed service account (the server masks its token, so none is returned)."""

    email: str
    first_name: str = ""
    last_name: str = ""
    role: str = ""
    created_at: datetime | None = None


@dataclass(frozen=True)
class Passcode:
    """The caller's ingest-only credential: Basic ``user:passcode`` may ingest but not search
    or administer."""

    user: str
    passcode: Secret


def _token(ctx: Ctx, data: object) -> ServiceAccountToken:
    body = as_dict(data)
    token = as_str(body.get("token")) or ""
    ctx.add_secret(token)
    return ServiceAccountToken(email=as_str(body.get("user")) or "", token=Secret(token))


def _check_email(email: str, operation: str) -> str:
    if not isinstance(email, str) or not _EMAIL.match(email.strip()):
        raise ValidationError(f"{operation}: email is not a valid address")
    return email.strip()


def _sa_path(config: OpenObserveConfig, email: str) -> str:
    return org_path(config, "service_accounts", email)


def list_organizations_call(config: OpenObserveConfig) -> Call[list[Organization]]:
    resp = yield request(config, "GET", "/api/organizations", "admin.list_organizations")
    return [
        Organization(
            id=as_int(o.get("id")),
            identifier=as_str(o.get("identifier")) or "",
            name=as_str(o.get("name")) or "",
            type=as_str(o.get("type")) or "",
            user_email=as_str(o.get("user_email")) or "",
        )
        for o in (as_dict(raw) for raw in as_list(as_dict(json_body(resp)).get("data")))
    ]


def create_service_account_call(
    ctx: Ctx, email: str, first_name: str, last_name: str
) -> Call[ServiceAccountToken]:
    op = "admin.create_service_account"
    address = _check_email(email, op)
    resp = yield request(
        ctx.config,
        "POST",
        org_path(ctx.config, "service_accounts"),
        op,
        json_body={"email": address, "first_name": first_name, "last_name": last_name},
    )
    return _token(ctx, json_body(resp))


def list_service_accounts_call(config: OpenObserveConfig) -> Call[list[ServiceAccount]]:
    resp = yield request(
        config, "GET", org_path(config, "service_accounts"), "admin.list_service_accounts"
    )
    out = []
    for raw in as_list(as_dict(json_body(resp)).get("data")):
        item = as_dict(raw)
        created = as_int(item.get("created_at"))
        out.append(
            ServiceAccount(
                email=as_str(item.get("email")) or "",
                first_name=as_str(item.get("first_name")) or "",
                last_name=as_str(item.get("last_name")) or "",
                role=as_str(item.get("role")) or "",
                created_at=micros_to_dt(created) if created > 0 else None,
            )
        )
    return out


def rotate_service_account_token_call(ctx: Ctx, email: str) -> Call[ServiceAccountToken]:
    op = "admin.rotate_service_account_token"
    if not email or not email.strip():
        raise ValidationError(f"{op}: email is required")
    resp = yield request(
        ctx.config,
        "PUT",
        _sa_path(ctx.config, email.strip()),
        op,
        params={"rotateToken": "true"},
        json_body={},
    )
    return _token(ctx, json_body(resp))


def delete_service_account_call(config: OpenObserveConfig, email: str) -> Call[None]:
    op = "admin.delete_service_account"
    if not email or not email.strip():
        raise ValidationError(f"{op}: email is required")
    yield request(config, "DELETE", _sa_path(config, email.strip()), op)
    return None


def get_passcode_call(ctx: Ctx) -> Call[Passcode]:
    resp = yield request(ctx.config, "GET", org_path(ctx.config, "passcode"), "admin.get_passcode")
    data = as_dict(as_dict(json_body(resp)).get("data"))
    passcode = as_str(data.get("passcode")) or ""
    ctx.add_secret(passcode)
    return Passcode(user=as_str(data.get("user")) or "", passcode=Secret(passcode))


class OpenObserveAdmin:
    """Admin helpers (``client.admin``). Every method accepts ``cancel=``.

    OSS OpenObserve has no RBAC: every service account can ingest, query and administer.
    """

    def __init__(self, ctx: Ctx) -> None:
        self._ctx = ctx

    def _run(self, call: Call[_T], cancel: CancelToken | None) -> _T:
        return run_sync(self._ctx, call, cancel)

    def list_organizations(self, *, cancel: CancelToken | None = None) -> list[Organization]:
        """Organizations visible to the caller (``GET /api/organizations``), including the
        internal ``_meta`` organization."""
        return self._run(list_organizations_call(self._ctx.config), cancel)

    def create_service_account(
        self,
        email: str,
        *,
        first_name: str = "",
        last_name: str = "",
        cancel: CancelToken | None = None,
    ) -> ServiceAccountToken:
        """Create a service account and return its token (``POST /api/{org}/service_accounts``).

        :class:`ConflictError` when it already exists, :class:`ValidationError` for an invalid
        e-mail.
        """
        return self._run(
            create_service_account_call(self._ctx, email, first_name, last_name), cancel
        )

    def list_service_accounts(self, *, cancel: CancelToken | None = None) -> list[ServiceAccount]:
        """The organization's service accounts (``GET /api/{org}/service_accounts``)."""
        return self._run(list_service_accounts_call(self._ctx.config), cancel)

    def rotate_service_account_token(
        self, email: str, *, cancel: CancelToken | None = None
    ) -> ServiceAccountToken:
        """Issue a new token; the old one stops working at once
        (``PUT /api/{org}/service_accounts/{email}?rotateToken=true``)."""
        return self._run(rotate_service_account_token_call(self._ctx, email), cancel)

    def delete_service_account(self, email: str, *, cancel: CancelToken | None = None) -> None:
        """Remove a service account (``DELETE /api/{org}/service_accounts/{email}``);
        :class:`NotFoundError` when it does not exist."""
        self._run(delete_service_account_call(self._ctx.config, email), cancel)

    def get_passcode(self, *, cancel: CancelToken | None = None) -> Passcode:
        """The caller's ingest-only passcode (``GET /api/{org}/passcode``)."""
        return self._run(get_passcode_call(self._ctx), cancel)


class AsyncOpenObserveAdmin:
    """Async :class:`OpenObserveAdmin`; same methods, awaited."""

    def __init__(self, ctx: Ctx) -> None:
        self._ctx = ctx

    async def _run(self, call: Call[_T]) -> _T:
        return await run_async(self._ctx, call)

    async def list_organizations(self) -> list[Organization]:
        """See :meth:`OpenObserveAdmin.list_organizations`."""
        return await self._run(list_organizations_call(self._ctx.config))

    async def create_service_account(
        self, email: str, *, first_name: str = "", last_name: str = ""
    ) -> ServiceAccountToken:
        """See :meth:`OpenObserveAdmin.create_service_account`."""
        return await self._run(create_service_account_call(self._ctx, email, first_name, last_name))

    async def list_service_accounts(self) -> list[ServiceAccount]:
        """See :meth:`OpenObserveAdmin.list_service_accounts`."""
        return await self._run(list_service_accounts_call(self._ctx.config))

    async def rotate_service_account_token(self, email: str) -> ServiceAccountToken:
        """See :meth:`OpenObserveAdmin.rotate_service_account_token`."""
        return await self._run(rotate_service_account_token_call(self._ctx, email))

    async def delete_service_account(self, email: str) -> None:
        """See :meth:`OpenObserveAdmin.delete_service_account`."""
        await self._run(delete_service_account_call(self._ctx.config, email))

    async def get_passcode(self) -> Passcode:
        """See :meth:`OpenObserveAdmin.get_passcode`."""
        return await self._run(get_passcode_call(self._ctx))
