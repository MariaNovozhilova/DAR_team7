def copy_table:
				import psycopg
				import json
				
				tables = ['базы_данных','базы_данных_и_уровень_знаний_сотру', 'инструменты', 'инструменты_и_уровень_знаний_сотр',
				         'образование_пользователей', 'опыт_сотрудника_в_отраслях', 'опыт_сотрудника_в_предметных_обла', 
				         'отрасли', 'платформы', 'платформы_и_уровень_знаний_сотруд', 'предметная_область', 'резюмедар', 
				         'сертификаты_пользователей', 'сотрудники_дар', 'среды_разработки', 'среды_разработки_и_уровень_знаний_', 
				         'технологии', 'технологии_и_уровень_знаний_сотру', 'типы_систем', 'типы_систем_и_уровень_знаний_сотру', 
				         'уровень_образования', 'уровни_владения_ин', 'уровни_знаний', 'уровни_знаний_в_отрасли', 
				         'уровни_знаний_в_предметной_област', 'фреймворки', 'фреймворки_и_уровень_знаний_сотру', 'языки',
				         'языки_пользователей', 'языки_программирования', 'языки_программирования_и_уровень']
				
				f = open('source_base.json')
				g = open('etl_base.json')
				dsn_source = json.load(f)
				dsn_ods = json.load(g)
				
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
				
				connection_source = create_connection(dsn_source["db_name"], dsn_source["db_user"], dsn_source["db_password"], dsn_source["db_host"], dsn_source["db_port"])
				connection_new = create_connection(dsn_ods["db_name"], dsn_ods["db_user"], dsn_ods["db_password"], dsn_ods["db_host"], dsn_ods["db_port"])
				
				def write_data(table):
				    print(f'Copy data from {table}')
				    with connection_source as conn1, connection_new as conn2:
				        with conn1.cursor().copy(f'COPY source_data."{table}" TO STDOUT') as copy1:
				            with conn2.cursor().copy(f'COPY ods."{table}" FROM STDIN') as copy2:
				                for data in copy1:
				                    copy2.write(data)
				
				for table in tables:
				    try: 
				        write_data(table)
				        print(f'{table} finished')
				    except Exception as e:
				        connection_source = create_connection(dsn_source["db_name"], dsn_source["db_user"], dsn_source["db_password"], dsn_source["db_host"], dsn_source["db_port"])
				        connection_new = create_connection(dsn_ods["db_name"], dsn_ods["db_user"], dsn_ods["db_password"], dsn_ods["db_host"], dsn_ods["db_port"])
				        write_data(table)
				
				connection_new.commit()
				connection_new.close()
				connection_source.close()
				
				return 'Done'				
