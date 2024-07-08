import psycopg

tables = ['''
         'базы_данных', 'базы_данных_и_уровень_знаний_сотру', 'инструменты', 'инструменты_и_уровень_знаний_сотр',
         'образование_пользователей', 'опыт_сотрудника_в_отраслях', 'опыт_сотрудника_в_предметных_обла', 
         'отрасли', 'платформы', 'платформы_и_уровень_знаний_сотруд', 'предметная_область', 'резюмедар', 
         'сертификаты_пользователей', 'сотрудники_дар', 'среды_разработки', 'среды_разработки_и_уровень_знаний_', 
         'технологии' , 'технологии_и_уровень_знаний_сотру', 'типы_систем', 'типы_систем_и_уровень_знаний_сотру', 
         'уровень_образования', 'уровни_владения_ин', 'уровни_знаний', 'уровни_знаний_в_отрасли', 
         'уровни_знаний_в_предметной_област', 'фреймворки', 'фреймворки_и_уровень_знаний_сотру', 'языки',
         'языки_пользователей', 'языки_программирования', 'языки_программирования_и_уровень'
'''
]

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

connection_source = create_connection(db_name = 'source', db_user = 'etl_user_7', db_password = 'MjT3-%>s', db_host = '10.82.0.4', db_port = '5432')
connection_new = create_connection(db_name = 'etl_db_7', db_user = 'etl_user_7', db_password = 'MjT3-%>s', db_host = '10.82.0.4', db_port = '5432')


with connection_source as conn1, connection_new as conn2:
    conn2.cursor().execute('''
                CREATE TABLE IF NOT EXISTS etl_db_7."базы_данных" (
     	        "название" character varying(50) COLLATE pg_catalog."default",
     "активность" character varying(50) COLLATE pg_catalog."default",
     "Сорт." integer,
     "Дата изм." character varying(50) COLLATE pg_catalog."default",
     id integer
);
                ''')
    with conn1.cursor().copy('COPY source_data."базы_данных" TO STDOUT') as copy1:
        with conn2.cursor().copy('COPY etl_db_7."базы_данных" FROM STDIN') as copy2:
            for data in copy1:
                copy2.write(data)


connection_new.commit()
cur_new.close()
connection_new.close()
connection_source.close()

