"""client.admin against the real service (service accounts are created and removed)."""

from __future__ import annotations

import pytest

import grow_openobserve as sdk


def test_list_organizations(client: sdk.OpenObserveClient) -> None:
    orgs = client.admin.list_organizations()
    assert client.config.org in [o.identifier for o in orgs]


def test_service_account_lifecycle(
    client: sdk.OpenObserveClient, env: dict[str, str], rid: str
) -> None:
    email = f"sdk-py-{rid}@stack.local"
    created = client.admin.create_service_account(email, first_name="SDK", last_name=rid)
    try:
        assert created.email == email
        assert repr(created.token) != created.token.reveal()
        with pytest.raises(sdk.ConflictError):
            client.admin.create_service_account(email)
        with sdk.OpenObserveClient.from_env(env, user=email, token=created.token.reveal()) as sa:
            assert sa.ping() >= 0

        listed = {a.email: a for a in client.admin.list_service_accounts()}
        assert listed[email].last_name == rid
        assert listed[email].created_at is not None

        rotated = client.admin.rotate_service_account_token(email)
        assert rotated.token.reveal() != created.token.reveal()
        with sdk.OpenObserveClient.from_env(env, user=email, token=created.token.reveal()) as old:
            with pytest.raises(sdk.AuthenticationError) as info:
                old.ping()
            assert created.token.reveal() not in str(info.value)
        with sdk.OpenObserveClient.from_env(env, user=email, token=rotated.token.reveal()) as new:
            assert new.ping() >= 0
    finally:
        client.admin.delete_service_account(email)
    with pytest.raises(sdk.NotFoundError):
        client.admin.delete_service_account(email)
    with pytest.raises(sdk.ValidationError):
        client.admin.create_service_account("not-an-email")


def test_passcode_ingests_only(
    client: sdk.OpenObserveClient, env: dict[str, str], stream: str
) -> None:
    passcode = client.admin.get_passcode()
    assert passcode.user == client.config.user
    ingest_only = sdk.OpenObserveClient.from_env(env, password=passcode.passcode.reveal())
    with ingest_only:
        assert ingest_only.logs.ingest_json(stream, [{"message": "via passcode"}]).successful == 1
        with pytest.raises(sdk.AuthenticationError):
            ingest_only.logs.search(f'SELECT * FROM "{stream}"')
    rows = client.logs.search(f'SELECT * FROM "{stream}"').hits
    assert [r["message"] for r in rows] == ["via passcode"]
