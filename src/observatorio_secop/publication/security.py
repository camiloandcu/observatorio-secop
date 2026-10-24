"""Dedicated publication-role bootstrap; no privileged consumer functions."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql

from .errors import PublicationError
from .models import BUILDER_ROLE, OUTPUT_RELATIONS, OWNER_ROLE, READER_ROLES, one


def bootstrap_roles(cursor: psycopg.Cursor[Any]) -> None:
    for role in (OWNER_ROLE, BUILDER_ROLE, *READER_ROLES):
        cursor.execute(
            "SELECT rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,rolcanlogin "
            "FROM pg_roles WHERE rolname=%s",
            (role,),
        )
        existing = cursor.fetchone()
        login = role in READER_ROLES
        if existing is None:
            cursor.execute(
                sql.SQL(
                    "CREATE ROLE {} {} NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(role), sql.SQL("LOGIN" if login else "NOLOGIN"))
            )
        elif any(existing[:5]) or existing[5] != login:
            raise PublicationError("PUBLICATION_ROLE_UNSAFE", "Existing publication role is unsafe")
        cursor.execute(
            "SELECT parent.rolname FROM pg_auth_members m JOIN pg_roles child "
            "ON child.oid=m.member JOIN pg_roles parent ON parent.oid=m.roleid "
            "WHERE child.rolname=%s",
            (role,),
        )
        if cursor.fetchall():
            raise PublicationError(
                "PUBLICATION_ROLE_UNSAFE", "Publication roles must not inherit other roles"
            )
    # Only the already-authorized database operator can administer publication ownership.
    cursor.execute("SELECT current_user")
    operator = one(cursor)[0]
    if operator in (*READER_ROLES, OWNER_ROLE, BUILDER_ROLE):
        raise PublicationError("PUBLICATION_DENIED", "Bootstrap requires the database operator")
    cursor.execute(
        sql.SQL("GRANT {}, {} TO {}").format(
            sql.Identifier(OWNER_ROLE), sql.Identifier(BUILDER_ROLE), sql.Identifier(operator)
        )
    )
    for schema in ("publication_control", "published"):
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(
                sql.Identifier(schema), sql.Identifier(OWNER_ROLE)
            )
        )
        cursor.execute(
            "SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner "
            "WHERE n.nspname=%s",
            (schema,),
        )
        if one(cursor)[0] != OWNER_ROLE:
            raise PublicationError(
                "PUBLICATION_ROLE_UNSAFE", "Existing publication schema owner is unsafe"
            )
        cursor.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(schema))
        )
        for role in (*READER_ROLES, BUILDER_ROLE):
            cursor.execute(
                sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(
                    sql.Identifier(schema), sql.Identifier(role)
                )
            )
    cursor.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    for role in (OWNER_ROLE, BUILDER_ROLE):
        cursor.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {} REVOKE ALL ON TABLES FROM PUBLIC").format(
                sql.Identifier(role)
            )
        )
        cursor.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"
            ).format(sql.Identifier(role))
        )


def grant_consumption(cursor: psycopg.Cursor[Any], schema: str) -> None:
    cursor.execute(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname=%s",
        (schema,),
    )
    if one(cursor)[0]:
        raise PublicationError(
            "PUBLICATION_ROLE_UNSAFE", "Consumption namespaces cannot contain functions"
        )
    expected = {*OUTPUT_RELATIONS, "publication_metadata"}
    if schema == "published":
        expected.add("catalog")
    cursor.execute(
        "SELECT c.relname,c.relkind FROM pg_class c JOIN pg_namespace n "
        "ON n.oid=c.relnamespace WHERE n.nspname=%s "
        "AND c.relkind IN ('r','v','m','p','f')",
        (schema,),
    )
    relations = cursor.fetchall()
    if any(name not in expected or kind != "v" for name, kind in relations):
        raise PublicationError(
            "PUBLICATION_ROLE_UNSAFE", "Consumption namespace contains unexpected relations"
        )
    for role in READER_ROLES:
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )
        for name, _ in relations:
            cursor.execute(
                sql.SQL("GRANT SELECT ON {}.{} TO {}").format(
                    sql.Identifier(schema), sql.Identifier(name), sql.Identifier(role)
                )
            )


def verify_reader_boundary(cursor: psycopg.Cursor[Any]) -> None:
    """Reject existing direct/PUBLIC grants instead of assuming new roles imply safety."""
    for role in READER_ROLES:
        cursor.execute(
            "SELECT has_database_privilege(%s,current_database(),'CREATE') OR EXISTS("
            "SELECT 1 FROM pg_namespace WHERE nspname !~ '^pg_' "
            "AND nspname<>'information_schema' AND has_schema_privilege(%s,oid,'CREATE'))",
            (role, role),
        )
        if one(cursor)[0]:
            raise PublicationError("PUBLICATION_ROLE_UNSAFE", "Reader has forbidden DDL privileges")
        cursor.execute(
            "SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls "
            "OR NOT rolcanlogin FROM pg_roles WHERE rolname=%s",
            (role,),
        )
        if one(cursor)[0]:
            raise PublicationError("PUBLICATION_ROLE_UNSAFE", "Reader role flags changed")
        cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member "
            "WHERE r.rolname=%s)",
            (role,),
        )
        if one(cursor)[0]:
            raise PublicationError("PUBLICATION_ROLE_UNSAFE", "Reader role membership changed")
        cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n "
            "ON n.oid=p.pronamespace WHERE p.prosecdef "
            "AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' "
            "AND has_schema_privilege(%s,n.oid,'USAGE') "
            "AND has_function_privilege(%s,p.oid,'EXECUTE'))",
            (role, role),
        )
        if one(cursor)[0]:
            raise PublicationError(
                "PUBLICATION_ROLE_UNSAFE", "Reader can execute an external privileged function"
            )
        cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n "
            "ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','v','m','f') "
            "AND n.nspname NOT IN ('pg_catalog','information_schema') "
            "AND n.nspname !~ '^pg_toast' AND ("
            "(NOT(c.relkind='v' AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=%s) "
            "AND n.nspowner=c.relowner AND ("
            "(n.nspname='published' AND c.relname=ANY(%s)) OR "
            "(c.relname=ANY(%s) AND EXISTS("
            "SELECT 1 FROM publication_control.publications p "
            "WHERE p.state='retained' AND p.ever_activated "
            "AND n.nspname='publication_'||p.publication_id)))) "
            "AND (has_table_privilege(%s,c.oid,'SELECT') OR "
            "has_any_column_privilege(%s,c.oid,'SELECT'))) OR "
            "has_table_privilege(%s,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR "
            "has_any_column_privilege(%s,c.oid,'INSERT,UPDATE,REFERENCES')))",
            (
                OWNER_ROLE,
                [*OUTPUT_RELATIONS, "publication_metadata", "catalog"],
                [*OUTPUT_RELATIONS, "publication_metadata"],
                role,
                role,
                role,
                role,
            ),
        )
        if one(cursor)[0]:
            raise PublicationError(
                "PUBLICATION_ROLE_UNSAFE", "Reader has forbidden effective privileges"
            )
