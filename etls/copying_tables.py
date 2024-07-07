from airflow.providers.postgres.hooks.postgres import PostgresHook


def copying_tables(src_schema: str, dest_schema: str, table: str) -> None:
    if not isinstance(src_schema, str) and isinstance(dest_schema, str) and isinstance(table, str):
        print(f'{src_schema} or {dest_schema} or {table} is not str')
        return

    src_table = src_schema + '.' + table
    dest_table = dest_schema + '.' + table

    src_hook = PostgresHook(postgres_conn_id='source')
    src_conn = src_hook.get_conn()
    src_cursor = src_conn.cursor()
    src_query = f'SELECT * FROM {src_table}'
    
    src_cursor.execute(src_query)
    data = src_cursor.fetchall()
    columns = [desc[0] for desc in src_cursor.description]
    print(columns)
    src_cursor.close()
    src_conn.close()

    dest_hook = PostgresHook(postgres_conn_id='etl_db_7')
    # dest_conn = dest_hook.get_conn()
    # dest_cursor = dest_conn.cursor()

    column_list = ', '.join([f'"{col}"' for col in columns])
    value = ', '.join(['%s'] * len(columns))
    dest_query = f"INSERT INTO {dest_table} ({column_list}) VALUES ({value})"
    
    for row in data:
        print(row)
        dest_hook.run(dest_query, parameters=row)
    
    # dest_cursor.close()
    # dest_conn.close()






    