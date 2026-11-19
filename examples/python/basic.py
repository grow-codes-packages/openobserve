"""Health, ping and server version with the synchronous client."""

from __future__ import annotations

import grow_openobserve as sdk


def main() -> None:
    # Reads OPENOBSERVE_URL/_USER/_PASSWORD (or GROW_OPENOBSERVE_*), timeouts … from the env.
    with sdk.OpenObserveClient.from_env() as client:
        status = client.health()
        print(f"healthy={status.healthy} status={status.status}")
        try:
            print(f"ping: {client.ping():.1f} ms")
            print(f"server version: {client.server_version()}")
        except sdk.AuthenticationError as err:
            print(f"credentials rejected: {err}")
        except sdk.OpenObserveError as err:
            print(f"failed with {err.code}: {err} (retryable={err.retryable})")


if __name__ == "__main__":
    main()
