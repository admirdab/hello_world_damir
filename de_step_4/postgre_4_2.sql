--Создание таблицы users
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
--Создание таблицы users_audit для логгирования изменений полей таблицы user
CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);
--Функция для логгирования изменений по полям таблицы users: name, email, role
create or replace function log_user_update()
returns trigger as $$
begin
	if old.name is distinct from new.name  then
	insert into users_audit (user_id, field_changed, old_value, new_value)
	values(new.id, 'name', old.name, new.name);
	end if;

	if old.email is distinct from new.email then
	insert into users_audit (user_id, field_changed, old_value, new_value)
	values(new.id, 'email', old.email, new.email);
	end if;

	if old.role is distinct from new.role  then
	insert into users_audit (user_id, field_changed, old_value, new_value)
	values(new.id, 'role', old.role, new.role);
	end if;	
return new;			   
end;
$$ language plpgsql

--триггер на таблицу users при изменений полей name, email, role
create trigger trigger_log_user_update
before update on users
for each row
execute function log_user_update();
------------------------------
--установка расширения pg_cron
create extension if not exists pg_cron;
--проверка установки
select * from pg_extension
show cron.database_name;
--функция для экспорта данных из users_audit на текущую дату
create or replace function export_current_date_user_audit_tocsv()
returns text as $$
declare
	file_name text;
	result_message text;
begin
	file_name := '/tmp/users_audit_export_' || to_char(current_date, 'YYYY-MM-DD') || '.csv';
	
	execute format('copy (
				select * from users_audit
				where changed_at::date = current_date
				order by changed_at
				) to %L with csv header', file_name);
	result_message := 'Данные users_audit за ' || to_char(current_date, 'YYYY-MM-DD') ||
					  ' экспортированы в файл: ' || file_name;
	return result_message;
end;
$$ language plpgsql;
---добавление функции export_current_date_user_audit_tocs в планировщик pg_cron
select cron.schedule(
    '3am_user_audit_export',  
    '0 3 * * *',                 
    $$select export_current_date_user_audit_tocsv()$$  
);
---проверка добавленного в планировщик
select * from cron.job
