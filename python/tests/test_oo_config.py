"""OpenObserveConfig: env precedence, auth rules, Basic header, redaction."""

from __future__ import annotations

import base64

import pytest

import grow_openobserve as sdk


def basic(user: str, secret: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{secret}".encode()).decode()


def test_defaults() -> None:
    cfg = sdk.OpenObserveConfig()
    assert (cfg.url, cfg.org, cfg.stream, cfg.traces_stream) == (
        "http://127.0.0.1:5080",
        "default",
        "default",
        "default",
    )
    assert cfg.user is None and not cfg.has_credentials
    assert cfg.authorization() is None
    assert cfg.known_secrets() == []


def test_env_precedence_and_vendor_names() -> None:
    env = {
        "OPENOBSERVE_URL": "http://vendor:5080",
        "OPENOBSERVE_ORG": "acme",
        "OPENOBSERVE_USER": "root@example.com",
        "OPENOBSERVE_PASSWORD": "vendor-pass",
        "OPENOBSERVE_STREAM": "App-Logs.v1",
        "OPENOBSERVE_TRACES_STREAM": "Traces",
        "GROW_OPENOBSERVE_URL": "http://grow:5080",
    }
    cfg = sdk.OpenObserveConfig.from_env(env)
    assert cfg.url == "http://grow:5080"
    assert cfg.org == "acme"
    assert cfg.stream == "app_logs_v1"
    assert cfg.traces_stream == "traces"
    assert cfg.password is not None and cfg.password.reveal() == "vendor-pass"
    assert cfg.authorization() == basic("root@example.com", "vendor-pass")
    explicit = sdk.OpenObserveConfig.from_env(env, org="other", stream="s")
    assert (explicit.org, explicit.stream) == ("other", "s")


def test_explicit_credential_kind_replaces_env_kind() -> None:
    env = {"OPENOBSERVE_USER": "u@example.com", "OPENOBSERVE_PASSWORD": "pw-123"}
    cfg = sdk.OpenObserveConfig.from_env(env, token="tok-123")
    assert cfg.password is None and cfg.token is not None
    env2 = {"OPENOBSERVE_USER": "u@example.com", "OPENOBSERVE_TOKEN": "tok-123"}
    cfg2 = sdk.OpenObserveConfig.from_env(env2, password="pw-123")
    assert cfg2.token is None and cfg2.password is not None
    with pytest.raises(sdk.ConfigError, match="not both"):
        sdk.OpenObserveConfig.from_env({**env, "OPENOBSERVE_TOKEN": "tok-123"})


def test_token_auth_and_pre_encoded_header() -> None:
    cfg = sdk.OpenObserveConfig(user="sa@example.com", token="sa-token-1")
    assert cfg.authorization() == basic("sa@example.com", "sa-token-1")
    assert "sa-token-1" in cfg.known_secrets()
    assert basic("sa@example.com", "sa-token-1")[6:] in cfg.known_secrets()
    pre = sdk.OpenObserveConfig(token="basic   abc123==")
    assert pre.authorization() == "Basic abc123=="
    assert "abc123" not in repr(pre)


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"org": ""}, "org"),
        ({"org": "a/b"}, "org"),
        ({"stream": "  "}, "stream"),
        ({"traces_stream": ""}, "traces_stream"),
        ({"user": "u@x", "password": "p", "token": "t"}, "not both"),
        ({"user": "u@x", "password": ""}, "blank"),
        ({"password": "pw"}, "user is required"),
        ({"token": "raw-token"}, "user is required"),
        ({"user": "u@x"}, "requires password or token"),
    ],
)
def test_validate(kwargs: dict[str, str], fragment: str) -> None:
    with pytest.raises(sdk.ConfigError) as info:
        sdk.OpenObserveConfig(**kwargs).validate()  # type: ignore[arg-type]
    assert fragment in str(info.value)
    for value in kwargs.values():
        if len(value) > 3:
            assert value not in str(info.value)


def test_repr_hides_credentials() -> None:
    cfg = sdk.OpenObserveConfig(user="u@example.com", password="pw-very-secret")
    assert "pw-very-secret" not in repr(cfg)
    assert "u@example.com" in repr(cfg)


def test_normalize_stream_name() -> None:
    assert sdk.normalize_stream_name(" Sdk-Mixed.x ") == "sdk_mixed_x"
