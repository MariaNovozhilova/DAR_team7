schema_init = 'CREATE SCHEMA IF NOT EXISTS g_ODS_test;'

tables_init = """
CREATE TABLE IF NOT EXISTS  g_ODS_test.базы_данных (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS  g_ODS_test.базы_данных_и_уровень_знаний_сотру (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY,
	"Базы данных" varchar(50) NULL,
	дата varchar(50) NULL,
	"Уровень знаний" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.инструменты (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.инструменты_и_уровень_знаний_сотр (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY,
	дата varchar(50) NULL,
	инструменты varchar(64) NULL,
	"Уровень знаний" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.образование_пользователей (
	"User ID" int4 NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY,
	"Уровень образование" text NULL,
	"Название учебного заведения" text NULL,
	"Фиктивное название" text NULL,
	"Факультет, кафедра" text NULL,
	специальность text NULL,
	квалификация text NULL,
	"Год окончания" int4 NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.опыт_сотрудника_в_отраслях (
	"User ID" int4 NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY,
	дата varchar(50) NULL,
	отрасли varchar(50) NULL,
	"Уровень знаний в отрасли" varchar(128) NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.опыт_сотрудника_в_предметных_обла (
	"User ID" int4 NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY,
	дата varchar(50) NULL,
	"Предментые области" varchar(50) NULL,
	"Уровень знаний в предметной облас" varchar(128) NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.отрасли (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.платформы (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.платформы_и_уровень_знаний_сотруд (
	"User ID" int4 NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY,
	дата varchar(50) NULL,
	платформы varchar(64) NULL,
	"Уровень знаний" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.предметная_область (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.резюмедар (
	"UserID" int4 NULL,
	"ResumeID" int4 PRIMARY KEY,
	"Активность" text NULL,
	"Образование" text NULL,
	"Сертификаты/Курсы" text NULL,
	"Языки" text NULL,
	"Базыданных" text NULL,
	"Инструменты" text NULL,
	"Отрасли" text NULL,
	"Платформы" text NULL,
	"Предметныеобласти" text NULL,
	"Средыразработки" text NULL,
	"Типысистем" text NULL,
	"Фреймворки" text NULL,
	"Языкипрограммирования" text NULL,
	"Технологии" text NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.сертификаты_пользователей (
	"User ID" int4 NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY,
	"Год сертификата" int4 NULL,
	"Наименование сертификата" text NULL,
	"Организация, выдавшая сертификат" text NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.сотрудники_дар (
	id int4 PRIMARY KEY,
	"Дата рождения" text NULL,
	активность text NULL,
	пол text NULL,
	фамилия text NULL,
	имя text NULL,
	"Последняя авторизация" text NULL,
	должность text NULL,
	цфо text NULL,
	"Дата регистрации" text NULL,
	"Дата изменения" text NULL,
	подразделения text NULL,
	"E-Mail" text NULL,
	логин text NULL,
	компания text NULL,
	"Город проживания" text NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.среды_разработки (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.среды_разработки_и_уровень_знаний_ (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY,
	дата varchar(50) NULL,
	"Среды разработки" varchar(50) NULL,
	"Уровень знаний" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.технологии (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.технологии_и_уровень_знаний_сотру (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY,
	дата text NULL,
	технологии text NULL,
	"Уровень знаний" text NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.типы_систем (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.типы_систем_и_уровень_знаний_сотру (
	название varchar(50) NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 PRIMARY KEY,
	дата varchar(50) NULL,
	"Типы систем" varchar(64) NULL,
	"Уровень знаний" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.уровень_образования (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.уровни_владения_ин (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.уровни_знаний (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.уровни_знаний_в_отрасли (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.уровни_знаний_в_предметной_област (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.фреймворки (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.фреймворки_и_уровень_знаний_сотру (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY,
	дата text NULL,
	"Уровень знаний" text NULL,
	фреймворки text NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.языки (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.языки_пользователей (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY,
	язык text NULL,
	"Уровень знаний ин. языка" text NULL
);

CREATE TABLE IF NOT EXISTS g_ODS_test.языки_программирования (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS g_ODS_test.языки_программирования_и_уровень (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 PRIMARY KEY,
	дата text NULL,
	"Уровень знаний" text NULL,
	"Языки программирования" text NULL
);
"""