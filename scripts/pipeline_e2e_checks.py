"""Code sent to the isolated runtime for safe-surface checks, never public payloads."""

AUDIT_CODE = r"""
import json,os,sys
from pathlib import Path
from urllib.parse import unquote,urlsplit
import psycopg
from psycopg import sql
from observatorio_secop.orchestration.models import Runtime,execution_id
from observatorio_secop.orchestration.operations import warehouse_config
from observatorio_secop.publication.models import (
    READER_ROLES,OUTPUT_RELATIONS,OWNER_ROLE,namespaces)

p=json.loads(sys.argv[1]); runtime=Runtime.environment(); config=warehouse_config(runtime)
secrets=[os.environ['POSTGRES_PASSWORD'],
         json.loads(Path('/opt/airflow/local-state/passwords.json').read_text())['admin'],
         Path('/opt/airflow/local-state/jwt-secret').read_text().strip()]
needles=[value.encode() for value in secrets if len(value)>8]
root=Path('/opt/airflow/logs'); total=0; files=0; leaked=False
for path in root.rglob('*'):
    if not path.is_file() or path.is_symlink():
        continue
    if not path.resolve().is_relative_to(root.resolve()):
        raise SystemExit(2)
    files+=1; previous=b''
    with path.open('rb') as stream:
        while chunk:=stream.read(65536):
            total+=len(chunk)
            if total>134217728:
                raise SystemExit(2)
            observed=previous+chunk
            leaked=leaked or any(value in observed for value in needles)
            previous=observed[-max(map(len,needles)):]
if not files or leaked:
    raise SystemExit(2)

metadata=urlsplit(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'].strip())
connection=dict(host=metadata.hostname,port=metadata.port or 5432,
                user=unquote(metadata.username),password=unquote(metadata.password),
                dbname=metadata.path.lstrip('/'),connect_timeout=10)
expected={run:execution_id('secop_pipeline',run) for run in p['runs']}
with psycopg.connect(**connection) as database:
    rows=database.execute('SELECT run_id,task_id,key,value FROM xcom '
                          'WHERE dag_id=%s AND run_id=ANY(%s)',
                          ('secop_pipeline',list(expected))).fetchall()
if len(rows)!=18 or any(key!='return_value' or value!=expected[run]
                        for run,task,key,value in rows):
    raise SystemExit(2)

checks=[]
for role in READER_ROLES:
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute(sql.SQL('SET SESSION AUTHORIZATION {}').format(sql.Identifier(role)))
        if database.execute('SELECT current_user').fetchone()[0]!=role:
            raise SystemExit(2)
        for relation in (*OUTPUT_RELATIONS,'publication_metadata'):
            database.execute(sql.SQL('SELECT count(*) FROM published.{}')
                             .format(sql.Identifier(relation))).fetchone()
        spaces=namespaces(p['publication_id'])
        denied=[sql.SQL('SELECT * FROM publication_control.active'),
                sql.SQL('SELECT * FROM silver.contract_current'),
                sql.SQL('SELECT * FROM {}.fact_contract').format(sql.Identifier(spaces['data'])),
                sql.SQL('DELETE FROM published.fact_contract WHERE false')]
        for statement in denied:
            try:
                with database.transaction():
                    database.execute(statement)
            except psycopg.errors.InsufficientPrivilege:
                pass
            else:
                raise SystemExit(2)
        if database.execute('SELECT pg_has_role(current_user,%s,%s)',
                            (OWNER_ROLE,'MEMBER')).fetchone()[0]:
            raise SystemExit(2)
        checks.append({'role':role,'published_reads':len(OUTPUT_RELATIONS)+1,
                       'denied_operations':len(denied),'owner_membership_denied':True})
print(json.dumps({'airflow_logs_secret_free':True,'log_files_checked':files,
                  'log_bytes_checked':total,'xcom_execution_ids_only':True,
                  'xcom_rows_checked':len(rows),'reader_acl_checks':checks}))
"""
