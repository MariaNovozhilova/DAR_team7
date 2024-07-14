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

tables = ['dbms', 'dbms_and_employee_level', 'domain', 'education', 'employee', 'employee_certificate', 
          'employee_domain_experience', 'industry', 'industry_employee_experience', 'platform', 
          'platform_and_employee_grade', 'program', 'program_and_employee_level', 'resume', 
          'sde', 'sde_and_employee_grade']

# loading bad_data into bad_data_layer
# эта часть кода создаёт таблицы в слое bad_data для ошибок с дополнительным столбцом
# сам скрипт по выявлению ошибок и записывание их в слой ещё не реализован
# for table in tables:
    # sql_load = (f"CREATE TABLE IF NOT EXISTS bad_data.{table} AS TABLE lgs.{table} WITH NO DATA;
    #             ALTER TABLE bad_data.{table} ADD COLUMN IF NOT EXISTS bad_data text;")
    # cur = conn.cursor()
    # cur.execute(sql_load)
    # conn.commit()

# создаю словарь с sql скриптами всех обработок и по очереди записываю в него запросы

SQL_dict = {
'sql_dael' : '''TRUNCATE TABLE dds.dbms_and_employee_level; INSERT INTO dds.dbms_and_employee_level
(id, user_id, updated_at, sort, grade, active, "date", dbms)
SELECT id
, CAST (regexp_replace(ldael.user_id, '[^0-9]', '', 'g') AS INTEGER)
, CAST(ldael.updated_at AS date)
, ldael.sort
, CASE WHEN ldael.grade = '' THEN '115637'
	   ELSE regexp_replace(ldael.grade, '[^0-9]', '', 'g') END
, CAST(CASE WHEN ldael.active = 'Да' THEN 'True'
			WHEN ldael.active = 'Нет' THEN 'False' END AS BOOL)
, CAST(CASE 
		when ldael."date" = '' then null end AS date) AS "date"
, regexp_replace(ldael.dbms, '[^0-9]', '', 'g')
FROM lgc.dbms_and_employee_level AS ldael;
'''}

SQL_dict['sql_d'] = ''' TRUNCATE TABLE dds.dbms;
INSERT INTO dds.dbms
(id, updated_at, sort, active, dbms)
SELECT ld.id
, CAST(ld.updated_at AS date)
, ld.sort
, CAST(CASE WHEN ld.active = 'Да' THEN 'True'
			WHEN ld.active = 'Нет' THEN 'False' END AS BOOL)
,  ld.dbms
FROM lgc.dbms AS ld;'''

SQL_dict['sql_dom'] =  '''TRUNCATE TABLE dds.domain; INSERT INTO dds.domain
(id, updated_at, sort, active, "domain")
SELECT ldom.id
, CAST(ldom.updated_at AS date)
, ldom.sort
, CAST(CASE WHEN ldom.active = 'Да' THEN 'True'
			WHEN ldom.active = 'Нет' THEN 'False' END AS BOOL)
,  ldom."domain"
FROM lgc.domain AS ldom;'''

SQL_dict['sql_le'] = '''TRUNCATE TABLE dds.education; INSERT INTO dds.education
(user_id, id, year_graduated, updated_at, institution_name, sort, education_level
, faculty_department, short_name, active, qualification, specialty)
SELECT le.user_id
, le.id
, le.year_graduated
, CAST(le.updated_at AS date)
, le.institution_name
, le.sort
, regexp_replace(le.education_level, '[^0-9]', '', 'g')
, le.faculty_department
, le.short_name
, CAST(CASE WHEN le.active = 'Да' THEN 'True'
			WHEN le.active = 'Нет' THEN 'False' END AS BOOL)
, le.qualification
, le.specialty
FROM lgc.education AS le;'''

SQL_dict['sql_lem'] = '''TRUNCATE TABLE dds.employee;
INSERT INTO dds.employee
(email, id, city, updated_at, registered_at, birth_date, last_check_in, active,
"position", "name", company, login, department, gender, surname, frc)
SELECT COALESCE (NULLIF(lem.email,''),'ivanivanych@korus.ru')
, lem.id
, COALESCE (NULLIF(lem.city,''),'Городок') AS city
, NULLIF(lem.updated_at,''):: date AS updated_at
, NULLIF(lem.registered_at,''):: date AS registered_at
, CAST(COALESCE (NULLIF(lem.birth_date,''),'1984-10-10') AS date) AS birth_date
, NULLIF(lem.last_check_in,''):: date AS last_check_in
, CAST(CASE WHEN lem.active = 'Да' THEN 'True'
			WHEN lem.active = 'Нет' THEN 'False' END AS BOOL)
, NULLIF(lem.position,'') AS "position"
, COALESCE (NULLIF(lem."name",''),'Иван') AS name
, lem.company
, lem.login
, lem.department
, COALESCE (NULLIF(lem.gender,''),'male') AS gender
, COALESCE (NULLIF(lem.surname,''),'Иванов') AS surname
, lem.frc
FROM lgc.employee AS lem;'''

SQL_dict['sql_lemc'] = '''TRUNCATE TABLE dds.employee_certificate;
INSERT INTO dds.employee_certificate
(user_id, id, year, updated_at, title, organisation, sort, active)
SELECT lemc.user_id
, lemc.id
, lemc.year
, NULLIF(lemc.updated_at,''):: date AS updated_at
, lemc.title
, lemc.organisation
, lemc.sort
, CAST(CASE WHEN lemc.active = 'Да' THEN 'True'
			WHEN lemc.active = 'Нет' THEN 'False' END AS BOOL)
FROM lgc.employee_certificate AS lemc;'''

SQL_dict['sql_lede'] = '''TRUNCATE TABLE dds.employee_domain_experience; INSERT INTO dds.employee_domain_experience
(user_id, id, updated_at, domain, sort, domain_experience, active, "date")
SELECT lede.user_id
, lede.id
, NULLIF(lede.updated_at,''):: date AS updated_at
, regexp_replace(lede.domain, '[^0-9]', '', 'g')
, lede.sort
, CASE WHEN lede.domain_experience = '' THEN '115761' 
ELSE regexp_replace(lede.domain_experience, '[^0-9]', '', 'g') END
, CAST(CASE WHEN lede.active = 'Да' THEN 'True'
			WHEN lede.active = 'Нет' THEN 'False' END AS BOOL)
, NULLIF(lede.date,''):: date
FROM lgc.employee_domain_experience AS lede;'''

SQL_dict['sql_li'] = '''TRUNCATE TABLE dds.industry; INSERT INTO dds.industry
(id, updated_at, sort, active, industry)
SELECT li.id
, NULLIF(li.updated_at,''):: date AS updated_at
, li.sort
, CAST(CASE WHEN li.active = 'Да' THEN 'True'
			WHEN li.active = 'Нет' THEN 'False' END AS BOOL)
,li.industry
FROM lgc.industry AS li;'''

SQL_dict['sql_liee'] = '''TRUNCATE TABLE dds.industry_employee_experience;
INSERT INTO dds.industry_employee_experience
(user_id, id, updated_at, sort, experience, active, "date", industry)
SELECT liee.user_id
, liee.id
, NULLIF(liee.updated_at,''):: date AS updated_at
, liee.sort
, CASE WHEN liee.experience = '' THEN '115761'
	   ELSE regexp_replace(liee.experience, '[^0-9]', '', 'g') END
, CAST(CASE WHEN liee.active = 'Да' THEN 'True'
			WHEN liee.active = 'Нет' THEN 'False' END AS BOOL)
, NULLIF(liee."date",''):: date AS "date"
, regexp_replace(liee.industry, '[^0-9]', '', 'g')
FROM lgc.industry_employee_experience AS liee;'''

SQL_dict['sql_lp'] = '''TRUNCATE TABLE dds.platform;
INSERT INTO dds.platform
(id, updated_at, sort, active, platform)
SELECT lp.id
, NULLIF(lp.updated_at,''):: date AS updated_at
, lp.sort
, CAST(CASE WHEN lp.active = 'Да' THEN 'True'
			WHEN lp.active = 'Нет' THEN 'False' END AS BOOL)
, lp.platform
FROM lgc.platform AS lp;'''

SQL_dict['sql_lpeg'] = '''TRUNCATE TABLE dds.platform_and_employee_grade;
INSERT INTO dds.platform_and_employee_grade
(user_id, id, updated_at, sort, grade, active, "date", platform)
SELECT lpeg.user_id
, lpeg.id
, NULLIF(lpeg.updated_at,''):: date AS updated_at
, lpeg.sort
, CASE WHEN lpeg.grade = '' THEN '115638'
		ELSE regexp_replace(lpeg.grade, '[^0-9]', '', 'g') END
, CAST(CASE WHEN lpeg.active = 'Да' THEN 'True'
			WHEN lpeg.active = 'Нет' THEN 'False' END AS BOOL)
, NULLIF(lpeg."date",''):: date AS "date"
, regexp_replace(lpeg.platform, '[^0-9]', '', 'g')
FROM lgc.platform_and_employee_grade AS lpeg;'''

SQL_dict['sql_program'] = '''TRUNCATE TABLE dds.program;
INSERT INTO dds.program
(id, updated_at, sort, active, program)
SELECT id
, NULLIF(updated_at,''):: date AS updated_at
, sort
, CAST(CASE WHEN active = 'Да' THEN 'True'
			WHEN active = 'Нет' THEN 'False' END AS BOOL)
, program
FROM lgc.program;'''

SQL_dict['sql_program_and_employee_level'] = '''TRUNCATE TABLE dds.program_and_employee_level;
INSERT INTO dds.program_and_employee_level
(id, updated_at, sort, grade, active, "date", program, user_id)
SELECT id
, NULLIF(updated_at,''):: date AS updated_at
, sort
, CASE WHEN grade = '' THEN '115638'
		ELSE regexp_replace(grade, '[^0-9]', '', 'g') END
, CAST(CASE WHEN active = 'Да' THEN 'True'
			WHEN active = 'Нет' THEN 'False' END AS BOOL)
, NULLIF("date",''):: date AS "date"			
, regexp_replace(program, '[^0-9]', '', 'g')
, CAST (regexp_replace(user_id, '[^0-9]', '', 'g') AS INTEGER)
FROM lgc.program_and_employee_level;'''

SQL_dict['sql_resume'] = '''TRUNCATE TABLE dds.resume;
INSERT INTO dds.resume
(id, user_id, active, dbms, program, education, industry, platform, domain, 
certificate, sde, tool, software_type, framework, language, programming_language)
SELECT id
, user_id
, CAST(CASE WHEN active = 'Да' THEN 'True'
			WHEN active = 'Нет' THEN 'False' END AS BOOL)
, NULLIF(dbms, '')
, NULLIF(program, '')
, NULLIF(education, '')
, NULLIF(industry, '')
, NULLIF(platform, '')
, NULLIF(domain, '')
, NULLIF(certificate, '')
, NULLIF(sde, '')
, NULLIF(tool, '')
, NULLIF(software_type, '')
, NULLIF(framework, '')
, NULLIF(language, '')
, NULLIF(programming_language, '')
FROM lgc.resume;'''

SQL_dict['sql_sde'] = '''TRUNCATE TABLE dds.sde;
INSERT INTO dds.sde (id, updated_at, sort, active, sde)
SELECT id
, NULLIF(updated_at,''):: date AS updated_at
, sort
, CAST(CASE WHEN active = 'Да' THEN 'True'
			WHEN active = 'Нет' THEN 'False' END AS BOOL)
, sde
FROM lgc.sde;'''

SQL_dict['sql_sde_and_employee_grade'] = '''TRUNCATE TABLE dds.sde_and_employee_grade;
INSERT INTO dds.sde_and_employee_grade (id, updated_at, sort, sde, grade, active, 
"date", user_id)
SELECT id
, NULLIF(updated_at,''):: date AS updated_at
, sort
, regexp_replace(sde, '[^0-9]', '', 'g')
, CASE WHEN grade = '' THEN '115638'
		ELSE regexp_replace(grade, '[^0-9]', '', 'g') END
, CAST(CASE WHEN active = 'Да' THEN 'True'
			WHEN active = 'Нет' THEN 'False' END AS BOOL)
, NULLIF("date",''):: date AS "date"
, CAST (regexp_replace(user_id, '[^0-9]', '', 'g') AS INTEGER)
FROM lgc.sde_and_employee_grade;'''

# в цикле запускаю выполнение всех запросов и заполняю dds слой очищенными данными
for sql in SQL_dict:
    sql = SQL_dict[sql]
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()