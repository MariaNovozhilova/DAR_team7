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

tables = ['personal_data', 'total_certificates', 'employee_skill']

SQL_DM_dict = {'total_certificates': '''
TRUNCATE TABLE dm.total_certificates; INSERT INTO dm.total_certificates (id, total_certificates, ranking)
SELECT e.id
, COALESCE(tc.total_certificates, 0) AS total_certificates
, DENSE_RANK() OVER (ORDER BY COALESCE(tc.total_certificates, 0) DESC) AS ranking
FROM dds.employee e
LEFT JOIN (SELECT ec.user_id
, COUNT(*) AS total_certificates
FROM dds.employee_certificate ec
GROUP BY ec.user_id) tc ON e.id = tc.user_id
ORDER BY 2 DESC;
'''}

SQL_DM_dict['personal_data'] = '''
TRUNCATE TABLE dm.personal_data; INSERT INTO dm.personal_data (id, name, surname, email, frc, position, ranking)
SELECT emp.id, emp.name, emp.surname, emp.email, emp.frc, emp."position", tc.ranking
FROM dds.employee emp
LEFT JOIN (
		SELECT e.id
		, COALESCE(tc.total_certificates, 0) AS total_certificates
		, DENSE_RANK() OVER (ORDER BY COALESCE(tc.total_certificates, 0) DESC) AS ranking
		FROM dds.employee e
		LEFT JOIN (SELECT ec.user_id
		, COUNT(*) AS total_certificates
		FROM dds.employee_certificate ec
		GROUP BY ec.user_id) tc ON e.id = tc.user_id ) tc ON tc.id = emp.id
'''
SQL_DM_dict['employee_skill'] = ''' TRUNCATE TABLE dm.employee_skill; INSERT INTO dm.employee_skill (id, 
skill, skill_detail, grade_level_exp, sort)
 SELECT e.id,
    'dbms'::text AS skill,
    d.dbms AS skill_detail,
    g.grade as grade_level_exp,
    deg.sort
   FROM dds.employee e
    -- только максимальные грейды  по каждой базе данных--
	 LEFT JOIN (SELECT * FROM dds.dbms_and_employee_grade
				WHERE dbms IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.dbms_and_employee_grade
				GROUP BY user_id, dbms)) deg ON e.id = deg.user_id
     LEFT JOIN dds.dbms d ON d.id = deg.dbms
     LEFT JOIN dds.grade g ON g.id = deg.grade

UNION
 
 SELECT e.id,
    'program'::text AS skill,
    p.program AS skill_detail,
    g.grade,
    peg.sort
   FROM dds.employee e
    -- только максимальные грейды  по каждой базе программе--
	 LEFT JOIN (SELECT * FROM dds.program_and_employee_grade
				WHERE program IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.program_and_employee_grade
				GROUP BY user_id, program)) peg ON e.id = peg.user_id
     LEFT JOIN dds.program p ON p.id = peg.program
     LEFT JOIN dds.grade g ON g.id = peg.grade

UNION

SELECT e.id,
    'platform'::text AS skill,
    pl.platform AS skill_detail,
    g.grade,
    pleg.sort
   FROM dds.employee e
     -- только максимальные грейды  по каждой платформе--
	 LEFT JOIN (SELECT * FROM dds.platform_and_employee_grade
				WHERE platform IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.platform_and_employee_grade
				GROUP BY user_id, platform)) pleg ON e.id = pleg.user_id
     LEFT JOIN dds.platform pl ON pl.id = pleg.platform
     LEFT JOIN dds.grade g ON g.id = pleg.grade

UNION

SELECT e.id,
    'tool'::text AS skill,
    t.tool AS skill_detail,
    g.grade,
    g.sort
   FROM dds.employee e
     -- только максимальные грейды  по каждой платформе--
	 LEFT JOIN (SELECT * FROM dds.tool_and_employee_grade
				WHERE tool IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.tool_and_employee_grade
				GROUP BY user_id, tool)) teg ON e.id = teg.user_id
     LEFT JOIN dds.tool t ON t.id = teg.tool
     LEFT JOIN dds.grade g ON g.id = teg.grade

UNION

SELECT e.id,
    'framework'::text AS skill,
    f.framework AS skill_detail,
    g.grade,
    g.sort
   FROM dds.employee e
    -- только максимальные грейды  по каждой платформе--
	 LEFT JOIN (SELECT * FROM dds.framework_and_employee_grade
				WHERE framework IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.framework_and_employee_grade
				GROUP BY user_id, framework)) feg ON e.id = feg.user_id
     LEFT JOIN dds.framework f ON f.id = feg.framework
     LEFT JOIN dds.grade g ON g.id = feg.grade

UNION

SELECT e.id,
    'domain'::text AS skill,
    d.domain AS skill_detail,
    exp.experience,
    exp.sort
   FROM dds.employee e
      -- только максимальные грейды  по каждой предметной области--
	 LEFT JOIN (SELECT * FROM dds.employee_domain_experience
				WHERE domain IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.employee_domain_experience
				GROUP BY user_id, domain)) ede ON e.id = ede.user_id
     LEFT JOIN dds.domain d ON d.id = ede.domain
     LEFT JOIN dds.experience exp ON exp.id = ede.experience

UNION

SELECT e.id,
    'industry'::text AS skill,
    i.industry AS skill_detail,
    iexp.experience,
    iexp.sort
   FROM dds.employee e
     LEFT JOIN dds.industry_employee_experience iee ON e.id = iee.user_id
     LEFT JOIN dds.industry i ON i.id = iee.industry
     LEFT JOIN dds.industry_experience iexp ON iexp.id = iee.experience

UNION
 
 SELECT e.id,
    'sde'::text AS skill,
    s.sde AS skill_detail,
    g.grade,
    seg.sort
   FROM dds.employee e
       -- только максимальные грейды  по каждой sde--
	 LEFT JOIN (SELECT * FROM dds.sde_and_employee_grade
				WHERE sde IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.sde_and_employee_grade
				GROUP BY user_id, sde)) seg ON e.id = seg.user_id
     LEFT JOIN dds.sde s ON s.id = seg.sde
     LEFT JOIN dds.grade g ON g.id = seg.grade; '''


# в цикле запускаю выполнение всех запросов и заполняю DM слой
for sql in SQL_DM_dict:
    sql = SQL_DM_dict[sql]
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()