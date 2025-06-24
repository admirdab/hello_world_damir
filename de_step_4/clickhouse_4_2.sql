--cоздание таблицы user_events
CREATE TABLE user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id);
--добавление TTL с учетом того, что данные хранятся 30 дней 
ALTER TABLE user_events
MODIFY TTL event_time + INTERVAL 30 DAY;

--создание агрегированной таблицы с интервалом хранения данных 180 дней
CREATE TABLE agg_user_events (
	event_date Date,
	event_type String,
	users AggregateFunction(uniq, UInt32),
	total_points AggregateFunction(sum, UInt32),
	total_actions AggregateFunction(count, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

--создание mv к таблицу agg_user_events на основе таблицы user_events 
--с промежуточным состоянием агрегации
CREATE MATERIALIZED VIEW mv_agg_user_events
TO agg_user_events
AS SELECT
	toDate(event_time) AS event_date
	,event_type
	,uniqState(user_id) AS users
	,sumState(points_spent) AS total_points
	,countState() AS total_actions
FROM user_events
GROUP BY event_date, event_type;
--вставка тестовых данных в таблицу user_events
INSERT INTO user_events VALUES

(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),


(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),


(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),


(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),


(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());


--запрос с быстрой аналитикой по дням
SELECT 
	event_date
	,event_type
	,uniqMerge(users) as unique_users
	,sumMerge(total_points) as total_spent
	,countMerge(total_actions) as total_actions
FROM agg_user_events
GROUP BY event_date, event_type 
ORDER BY event_date, event_type;
--расчет retention 7 дней, но не уверен в достоверности
WITH first_touch AS (
	SELECT
		user_id 
		,min(event_time) as first_day
	FROM user_events 
	WHERE event_type = 'login' or event_type = 'signup'
	GROUP BY user_events.user_id
)
,returned_in_7days AS (
	SELECT 
		DISTINCT ft.user_id
	FROM first_touch ft
	JOIN user_events ue ON ue.user_id = ft.user_id
	WHERE event_time BETWEEN ft.first_day  AND ft.first_day + INTERVAL 7 DAY
		AND (ue.event_type = 'login' or ue.event_type = 'purchase')
)
,returned_in_0day AS ( 
	SELECT 
		user_id
	FROM user_events
	WHERE event_type = 'signup' 
		AND toDate(event_time) = (SELECT toDate(MIN(event_time)) 
							  	  FROM user_events 
							  	  WHERE event_type = 'login')
)
SELECT
    (SELECT COUNT(DISTINCT user_id) FROM first_touch) AS total_users_day_0,
    (SELECT COUNT(DISTINCT user_id) FROM returned_in_7days) AS returned_in_7_days,
    ROUND(
        (SELECT COUNT(DISTINCT user_id) FROM returned_in_7days) * 100.0 /
        (SELECT COUNT(DISTINCT user_id) FROM first_touch),
        2
    ) AS retention_7d_percent	
	



















