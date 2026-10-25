from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from psycopg.types.json import Jsonb
except ImportError:
    try:
        from psycopg2.extras import Json as Jsonb
    except ImportError:
        import json

        class Jsonb:
            def __init__(self, val):
                self.val = val

            def __repr__(self):
                return json.dumps(self.val)

            def __str__(self):
                return json.dumps(self.val)


def get_connection(conn_id: str = "postgres_default"):
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn()


def extract_data(conn_id: str, sql: str):
    conn = get_connection(conn_id)
    cursor = conn.cursor()

    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    cursor.close()
    conn.close()
    return columns, rows


def truncate_table(conn_id: str, table_name: str):
    conn = get_connection(conn_id)
    cursor = conn.cursor()

    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")
    conn.commit()

    cursor.close()
    conn.close()


def load_data(
    conn_id: str,
    table_name: str,
    columns: list,
    rows: list,
):
    if not rows:
        print(f"No rows to load into {table_name}")
        return

    conn = get_connection(conn_id)
    cursor = conn.cursor()

    placeholders = ",".join(["%s"] * len(columns))
    column_names = ",".join(columns)

    insert_sql = f"""
        INSERT INTO {table_name}
        ({column_names})
        VALUES ({placeholders})
    """

    # Check if rows contain dict/list (e.g. JSONB columns) and wrap with Jsonb adapter
    has_json = False
    for row in rows[:100]:
        if any(isinstance(v, (dict, list)) for v in row):
            has_json = True
            break

    if has_json:
        prepared_rows = [
            tuple(Jsonb(v) if isinstance(v, (dict, list)) else v for v in row)
            for row in rows
        ]
    else:
        prepared_rows = rows

    # Use executemany instead of execute_batch to avoid mogrify error
    cursor.executemany(insert_sql, prepared_rows)

    conn.commit()
    cursor.close()
    conn.close()
