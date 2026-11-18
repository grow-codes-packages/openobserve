"""Admin helpers: organizations and a short-lived service account used as credentials."""

from __future__ import annotations

import grow_openobserve as sdk


def main() -> None:
    with sdk.OpenObserveClient.from_env() as admin:
        print("organizations:", [o.identifier for o in admin.admin.list_organizations()])
        account = admin.admin.create_service_account(
            "example-bot@example.com", first_name="Example"
        )
        try:
            # A service account authenticates with HTTP Basic email:token (never Bearer).
            config = sdk.OpenObserveConfig.from_env(
                user=account.email, token=account.token.reveal()
            )
            with sdk.OpenObserveClient(config) as bot:
                print(f"service account ping: {bot.ping():.1f} ms")
        finally:
            admin.admin.delete_service_account(account.email)


if __name__ == "__main__":
    main()
