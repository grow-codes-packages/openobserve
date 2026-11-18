"""client.admin with recorded responses; returned tokens are redacted afterwards."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

import grow_openobserve as sdk
from _support import MockServer, Reply, make_config, replay

SA = "/api/default/service_accounts"


def oo(server: MockServer) -> sdk.OpenObserveClient:
    return sdk.OpenObserveClient(make_config(server.url, user="u@example.com", password="pw"))


def test_organizations(server: MockServer) -> None:
    server.enqueue("/api/organizations", replay("listOrganizations.ok"))
    orgs = oo(server).admin.list_organizations()
    assert orgs[1] == sdk.Organization(1, "default", "default", "default", "admin@stack.local")


def test_service_account_lifecycle(server: MockServer) -> None:
    token = "tok-abcdef-123456"
    server.enqueue(
        SA,
        Reply(
            200,
            {"code": 200, "message": "User saved successfully", "token": token, "user": "a@b.io"},
        ),
        replay("createServiceAccount.400_duplicate"),
        replay("createServiceAccount.400_invalid_email"),
        replay("listServiceAccounts.ok"),
    )
    path = "/api/default/service_accounts/a%40b.io"
    server.enqueue(
        path,
        Reply(200, {"token": "rotated-777777", "user": "a@b.io"}),
        replay("deleteServiceAccount.ok"),
        Reply(404, {"code": 404, "message": f"User not found {token}"}),
    )
    c = oo(server)
    created = c.admin.create_service_account("a@b.io", first_name="A", last_name="B")
    assert created.email == "a@b.io" and created.token.reveal() == token
    assert token not in repr(created)
    assert json.loads(server.seen[-1].body) == {
        "email": "a@b.io",
        "first_name": "A",
        "last_name": "B",
    }
    with pytest.raises(sdk.ConflictError):
        c.admin.create_service_account("a@b.io")
    with pytest.raises(sdk.ValidationError):
        c.admin.create_service_account("x@y.io")
    with pytest.raises(sdk.ValidationError):
        c.admin.create_service_account("not-an-email")
    accounts = c.admin.list_service_accounts()
    assert accounts[0].email == "sdk-6moiazbq@stack.local"
    assert accounts[0].role == "service_account"
    assert accounts[0].created_at == datetime(2026, 11, 18, 20, 20, 0, 211546, tzinfo=timezone.utc)
    rotated = c.admin.rotate_service_account_token("a@b.io")
    assert rotated.token.reveal() == "rotated-777777"
    assert "rotateToken=true" in server.seen[-1].path
    c.admin.delete_service_account("a@b.io")
    with pytest.raises(sdk.NotFoundError) as info:
        c.admin.delete_service_account("a@b.io")
    assert token not in str(info.value)
    for call in (c.admin.rotate_service_account_token, c.admin.delete_service_account):
        with pytest.raises(sdk.ValidationError):
            call(" ")


def test_passcode(server: MockServer) -> None:
    server.enqueue(
        "/api/default/passcode",
        Reply(200, {"data": {"passcode": "pc-123456789", "user": "u@example.com"}}),
    )
    passcode = oo(server).admin.get_passcode()
    assert passcode.user == "u@example.com" and passcode.passcode.reveal() == "pc-123456789"


async def test_async_admin(server: MockServer) -> None:
    server.enqueue("/api/organizations", replay("listOrganizations.ok"))
    server.enqueue(
        SA, Reply(200, {"token": "t-1234567", "user": "a@b.io"}), replay("listServiceAccounts.ok")
    )
    server.enqueue(
        "/api/default/service_accounts/a%40b.io",
        Reply(200, {"token": "t-7654321", "user": "a@b.io"}),
        replay("deleteServiceAccount.ok"),
    )
    server.enqueue("/api/default/passcode", replay("getPasscode.ok"))
    async with sdk.AsyncOpenObserveClient(
        make_config(server.url, user="u@example.com", password="pw")
    ) as c:
        assert len(await c.admin.list_organizations()) == 2
        assert (await c.admin.create_service_account("a@b.io")).token.reveal() == "t-1234567"
        assert len(await c.admin.list_service_accounts()) == 2
        assert (await c.admin.rotate_service_account_token("a@b.io")).token.reveal() == "t-7654321"
        await c.admin.delete_service_account("a@b.io")
        assert (await c.admin.get_passcode()).user == "admin@stack.local"
