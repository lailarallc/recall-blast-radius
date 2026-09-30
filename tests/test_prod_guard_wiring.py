"""The prod guard sits in front of every write path to Postgres.

`pipeline/seed.py` and the `genealogy_seed` Dagster asset INSERT into raw.* and
genealogy.* at whatever DATABASE_URL names -- localhost:5432 locally, which is a
`fly proxy` tunnel to production when one is open. These tests fake a flyctl
listener and assert nothing connects.
"""

import runpy

import psycopg2
import pytest

from pipeline import prod_guard

LOCAL_URL = "postgresql://postgres@localhost:5432/cinderhaven"


@pytest.fixture
def fly_tunnel(monkeypatch):
    monkeypatch.delenv("ALLOW_PROD_DB", raising=False)
    monkeypatch.setattr(prod_guard, "_listener", lambda port: "flyctl")
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: pytest.fail("connected"))


def test_seed_refuses_fly_tunnel(fly_tunnel, monkeypatch):
    import pipeline.generate_genealogy as gen
    monkeypatch.setattr(gen, "generate_all", lambda: {})
    monkeypatch.setenv("DATABASE_URL", LOCAL_URL)
    with pytest.raises(prod_guard.ProdDatabaseError):
        runpy.run_module("pipeline.seed", run_name="__main__")


def test_seed_guards_the_configured_port(monkeypatch):
    import pipeline.generate_genealogy as gen
    seen = []
    monkeypatch.delenv("ALLOW_PROD_DB", raising=False)
    monkeypatch.setattr(prod_guard, "_listener", lambda port: seen.append(port) or "flyctl")
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: pytest.fail("connected"))
    monkeypatch.setattr(gen, "generate_all", lambda: {})
    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres@localhost:15432/cinderhaven")
    with pytest.raises(prod_guard.ProdDatabaseError):
        runpy.run_module("pipeline.seed", run_name="__main__")
    assert seen == [15432]


def test_assets_get_conn_refuses_fly_tunnel(fly_tunnel, monkeypatch):
    pytest.importorskip("dagster")
    from pipeline import assets
    monkeypatch.setenv("DATABASE_URL", LOCAL_URL)
    with pytest.raises(prod_guard.ProdDatabaseError):
        assets._get_conn()
