CREATE TABLE IF NOT EXISTS dm.total_certificates (id int PRIMARY KEY, total_certificates text, ranking integer);

CREATE TABLE IF NOT EXISTS dm.personal_data (id int primary key, name text, surname text, email text, 
frc text, position text, ranking integer);

CREATE TABLE IF NOT EXISTS dm.employee_skill (id int primary key, skill text, skill_detail text, 
grade_level_exp text, sort integer);


