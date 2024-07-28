import psycopg
import json
import pandas as pd
from datetime import datetime, timedelta

f = open('etl_base.json')
dsn_source = json.load(f)

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except psycopg.OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

conn = create_connection(dsn_source["db_name"], dsn_source["db_user"], dsn_source["db_password"], dsn_source["db_host"], dsn_source["db_port"])

tables = ['dbms', 'dbms_and_employee_grade', 'domain', 'employee_education_level', 'employee', 'employee_certificate', 
          'employee_domain_experience', 'industry', 'industry_employee_experience', 'platform', 
          'platform_and_employee_grade', 'program', 'program_and_employee_grade', 'resume', 
          'sde', 'sde_and_employee_grade']



for table in tables:
    sql = (f'SELECT (dds.{table}.*)::text,  count(*) FROM dds.{table} GROUP BY dds.{table}.* HAVING count(*) >1;')
    cur = conn.cursor()
    cur.execute(sql)
    print(cur.fetchall())
    conn.commit()