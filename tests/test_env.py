import os
from importlib import reload

import pytest

import mictlanxrouter.server as server


CRITICAL_ENV_VARS = [
    "MICTLANX_ROUTER_SERVICE_NAME",
    "MICTLANX_ROUTER_HOST",
    "MICTLANX_ROUTER_PORT",
    "MICTLANX_ROUTER_NETWORK_ID",
    "MICTLANX_API_VERSION",
    "MICTLANX_SUMMONER_IP_ADDR",
    "MICTLANX_DAEMON_HOSTNAME",
]


def test_required_env_vars_are_present():
    missing = []

    for var in CRITICAL_ENV_VARS:
        value = os.getenv(var)
        if value is None or value == "":
            missing.append(var)

    assert not missing, f"The following env vars are missing or empty: {missing}"


def test_server_reads_env_variables(monkeypatch):
    # Arrange: set valores fake en el entorno
    monkeypatch.setenv("MICTLANX_ROUTER_HOST", "test-host")
    monkeypatch.setenv("MICTLANX_ROUTER_PORT", "12345")
    monkeypatch.setenv("MICTLANX_ROUTER_SERVICE_NAME", "mictlanx-router-test")

    # Act: We reloaded the module so that it can read os.environ again
    reload(server)

    # Assert: we check that the module constants reflect the environment
    assert server.MICTLANX_ROUTER_HOST == "test-host"
    assert server.MICTLANX_ROUTER_PORT == 12345
    assert server.MICTLANX_ROUTER_SERVICE_NAME == "mictlanx-router-test"
