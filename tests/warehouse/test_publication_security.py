"""Effective ACL canaries on a dedicated disposable database, without dbt builds."""

from __future__ import annotations

import uuid
from dataclasses import replace

import psycopg
import pytest
from psycopg import sql

from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.models import READER_ROLES, namespaces
from observatorio_secop.publication.security import verify_reader_boundary

from .test_publication_acceptance import TEST_DSN
from .test_publication_acceptance import publication_database as _publication_database

pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires dedicated disposable PostgreSQL")
CANARY = "PRIVATE_SECURITY_CANARY"
PRIVATE = "publication_security_private"


@pytest.fixture
def security_database(tmp_path):
    yield from _publication_database.__wrapped__(tmp_path)


def _reader(config, role):
    password = uuid.uuid4().hex
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(
            sql.SQL("ALTER ROLE {} PASSWORD {}").format(sql.Identifier(role), sql.Literal(password))
        )
    return replace(config, user=role, password=password)


def _check(config, *, rejected):
    with (
        psycopg.connect(**config.connection_kwargs) as connection,
        connection.cursor() as cursor,
    ):
        if rejected:
            with pytest.raises(PublicationError) as failure:
                verify_reader_boundary(cursor)
            assert failure.value.code == "PUBLICATION_ROLE_UNSAFE"
        else:
            verify_reader_boundary(cursor)


def _private_table(connection):
    connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(PRIVATE)))
    connection.execute(
        sql.SQL("CREATE TABLE {}.secret(secret text)").format(sql.Identifier(PRIVATE))
    )
    connection.execute(
        sql.SQL("INSERT INTO {}.secret VALUES(%s)").format(sql.Identifier(PRIVATE)),
        (CANARY,),
    )


@pytest.mark.parametrize("role", READER_ROLES)
@pytest.mark.parametrize("public", (False, True))
@pytest.mark.parametrize("privilege", ("SELECT", "UPDATE"))
def test_column_only_private_grants_are_rejected(security_database, role, public, privilege):
    config, _, _ = security_database
    reader = _reader(config, role)
    grantee = sql.SQL("PUBLIC") if public else sql.Identifier(role)
    _check(config, rejected=False)
    with psycopg.connect(**config.connection_kwargs) as connection:
        _private_table(connection)
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(PRIVATE), grantee)
        )
        connection.execute(
            sql.SQL("GRANT {}(secret) ON {}.secret TO {}").format(
                sql.SQL(privilege), sql.Identifier(PRIVATE), grantee
            )
        )
    # Authenticate as the real consumer: a table-level-only check misses this path.
    with psycopg.connect(**reader.connection_kwargs) as connection:
        assert not connection.execute(
            "SELECT has_table_privilege(current_user,%s,%s)",
            (PRIVATE + ".secret", privilege),
        ).fetchone()[0]
        if privilege == "SELECT":
            assert connection.execute(
                sql.SQL("SELECT secret FROM {}.secret").format(sql.Identifier(PRIVATE))
            ).fetchone() == (CANARY,)
        else:
            connection.execute(
                sql.SQL("UPDATE {}.secret SET secret=%s").format(sql.Identifier(PRIVATE)),
                ("WRITTEN_CANARY",),
            )
    _check(config, rejected=True)


@pytest.mark.parametrize("role", READER_ROLES)
@pytest.mark.parametrize("public", (False, True))
def test_executable_external_security_definer_is_rejected(security_database, role, public):
    config, _, _ = security_database
    reader = _reader(config, role)
    with psycopg.connect(**config.connection_kwargs) as connection:
        _private_table(connection)
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(PRIVATE), sql.Identifier(role)
            )
        )
        connection.execute(
            sql.SQL(
                "CREATE FUNCTION {}.leak() RETURNS text LANGUAGE sql SECURITY DEFINER "
                "SET search_path=pg_catalog AS {}"
            ).format(
                sql.Identifier(PRIVATE),
                sql.Literal(f'SELECT secret FROM "{PRIVATE}".secret'),
            )
        )
        if not public:
            connection.execute(
                sql.SQL("REVOKE EXECUTE ON FUNCTION {}.leak() FROM PUBLIC").format(
                    sql.Identifier(PRIVATE)
                )
            )
            connection.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION {}.leak() TO {}").format(
                    sql.Identifier(PRIVATE), sql.Identifier(role)
                )
            )
    with (
        psycopg.connect(**reader.connection_kwargs) as connection,
        pytest.raises(psycopg.errors.InsufficientPrivilege),
    ):
        connection.execute(sql.SQL("SELECT secret FROM {}.secret").format(sql.Identifier(PRIVATE)))
    with psycopg.connect(**reader.connection_kwargs) as connection:
        assert connection.execute(
            sql.SQL("SELECT {}.leak()").format(sql.Identifier(PRIVATE))
        ).fetchone() == (CANARY,)
    _check(config, rejected=True)


@pytest.mark.parametrize("state", ("unregistered", "sealed"))
@pytest.mark.parametrize("kind", ("table", "view"))
def test_matching_namespace_is_not_a_publication_authorization(security_database, state, kind):
    config, store, _ = security_database
    role = READER_ROLES[0]
    reader = _reader(config, role)
    value = uuid.uuid4().hex if state == "unregistered" else store.reserve({})["publication_id"]
    schema = namespaces(value)["read"]
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {} AUTHORIZATION secop_publication_owner").format(
                sql.Identifier(schema)
            )
        )
        if state == "sealed":
            connection.execute(
                "UPDATE publication_control.publications SET state='sealed' "
                "WHERE publication_id=%s",
                (value,),
            )
        connection.execute(
            sql.SQL("CREATE {} {}.fact_contract AS SELECT {}::text AS secret").format(
                sql.SQL("TABLE" if kind == "table" else "VIEW"),
                sql.Identifier(schema),
                sql.Literal(CANARY),
            )
        )
        connection.execute(
            sql.SQL("ALTER {} {}.fact_contract OWNER TO secop_publication_owner").format(
                sql.SQL("TABLE" if kind == "table" else "VIEW"), sql.Identifier(schema)
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )
        connection.execute(
            sql.SQL("GRANT SELECT ON {}.fact_contract TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )
    with psycopg.connect(**reader.connection_kwargs) as connection:
        assert connection.execute(
            sql.SQL("SELECT secret FROM {}.fact_contract").format(sql.Identifier(schema))
        ).fetchone() == (CANARY,)
    _check(config, rejected=True)


def test_harmless_invoker_function_does_not_block_publication(security_database):
    config, _, _ = security_database
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(PRIVATE)))
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO PUBLIC").format(sql.Identifier(PRIVATE))
        )
        connection.execute(
            sql.SQL(
                "CREATE FUNCTION {}.harmless() RETURNS integer LANGUAGE sql "
                "SECURITY INVOKER AS 'SELECT 1'"
            ).format(sql.Identifier(PRIVATE))
        )
    _check(config, rejected=False)
