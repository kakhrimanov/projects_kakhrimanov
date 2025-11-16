-- Переименование таблицы
ALTER TABLE newtable
RENAME TO coins;

-- Меняем тип данных у колонки date и других колонок
ALTER TABLE coins
    ALTER COLUMN date TYPE DATE USING date::DATE,
    ALTER COLUMN high TYPE NUMERIC(20, 8) USING high::NUMERIC(20, 8),
    ALTER COLUMN low TYPE NUMERIC(20, 8) USING low::NUMERIC(20, 8),
    ALTER COLUMN open TYPE NUMERIC(20, 8) USING open::NUMERIC(20, 8),
    ALTER COLUMN close TYPE NUMERIC(20, 8) USING close::NUMERIC(20, 8),
    ALTER COLUMN volume TYPE NUMERIC(30, 8) USING volume::NUMERIC(30, 8),
    ALTER COLUMN marketcap TYPE NUMERIC(30, 8) USING marketcap::NUMERIC(30, 8);

-- Удаляем записи июля 21 года
DELETE FROM coins
WHERE date >= '2021-07-01';


-- Создаем материализованное представление по записям на последний день каждого месяца каждого года
CREATE MATERIALIZED VIEW coins_fd AS 
SELECT name, symbol, date, high, low, open, close, volume, marketcap, segment
FROM coins 
-- Где отбираем записи с последним имеющимся днем месяца по каждой монете
WHERE EXISTS (
	SELECT name, last_day_in_month
	FROM (
		-- Создаем таблицу, где представлена монета и запись наибольшей даты по ней в разрезе месяца каждого года
		SELECT name, MAX(date) AS last_day_in_month
		FROM coins
		GROUP BY name, DATE_TRUNC('month', date)
		ORDER BY last_day_in_month ASC
	) AS final_days
	WHERE final_days.name = coins.name AND final_days.last_day_in_month = coins.date
);

CREATE MATERIALIZED VIEW crypto_overview AS 
WITH
-- Доля рыночной капитализации монеты от общей капитализации рынка по месяцам
-- Ранжирование криптовалют по капитализации по месяцам
-- Общая капитализация сегмента по месяцам
-- Доля капитализации монеты от от капитализации сегмента по месяцам
t_crypto_months AS (
	SELECT name, symbol, 
		DATE_TRUNC('month', date)::DATE AS month, 
		high, low, open, close, 
		marketcap, segment, 
		SUM(marketcap) OVER w1 AS total_cap, -- Общая капитализация рынка в секции месяца
		DENSE_RANK() OVER w2 AS total_place, -- Номер криптовалюты в рейтинге по капитализации в секции месяца
		ROUND(marketcap * 100.0 / SUM(marketcap) OVER w1, 2) AS total_share, -- Доля капитализации криптовалюты от общей капитализации
		SUM(marketcap) OVER w3 AS segment_cap, -- Общая капитализация сегмента в секции месяца и сегмента
		round(marketcap * 100.0 / SUM(marketcap) OVER w3, 2) AS segment_share -- Доля капитализации криптовалюты от капитализации сегмента
	FROM coins_fd 
	WHERE marketcap != 0
	WINDOW w1 AS (PARTITION BY DATE_TRUNC('month', date)), 
		w2 AS (PARTITION BY DATE_TRUNC('month', date) ORDER BY marketcap DESC),
		w3 AS (PARTITION BY DATE_TRUNC('month', date), segment)
),
-- 1) Суммарный объем за месяц по каждой монете
-- 2) Ежемесячный общий объем за месяц
t_total_volume_coins AS (
	SELECT DATE_TRUNC('month', date)::DATE AS month, name, 
		SUM(volume) AS month_volume
	FROM coins
	GROUP BY DATE_TRUNC('month', date)::DATE, name
),
-- Объединение двух таблиц: t_crypto_months и t_total_volume_coins
t_crypto_metrics_1 AS (
	SELECT *
	FROM t_crypto_months
		LEFT JOIN t_total_volume_coins
		USING (name, month)
),
-- Общий объем по месяцам
t_total_volume AS (
	SELECT DATE_TRUNC('month', date)::DATE AS month, 
			-- Случай, когда торговый объем равен нулю изменим на NULL
		CASE 
			WHEN SUM(volume) = 0 THEN NULL
			ELSE SUM(volume)
		END AS total_month_volume -- Суммарный объем 
	FROM coins 
	GROUP BY DATE_TRUNC('month', date)::DATE
),
-- Разница по объемам
t_diff_volume AS (
	SELECT month, 
		total_month_volume,
		ROUND((total_month_volume - LAG(total_month_volume, 1) OVER w1), 2) AS diff_volume
	FROM t_total_volume
	WINDOW w1 AS (ORDER BY MONTH)
),
-- Капитализация по месяцам
t_total_marketcap AS (
	SELECT DATE_TRUNC('month', date)::DATE AS month, 
		SUM(marketcap) AS total_month_marketcap -- Общая капитализация 
	FROM coins_fd 
	GROUP BY DATE_TRUNC('month', date)::DATE
),
-- Разница по капитализации
t_diff_marketcap AS (
	SELECT month, 
		ROUND((total_month_marketcap - LAG(total_month_marketcap, 1) OVER w1), 2) AS diff_cap -- Разница по капитализации
	FROM t_total_marketcap
	WINDOW w1 AS (ORDER BY month)
	ORDER BY MONTH
)
-- Объединяем таблицы t_crypto_metrics_1, t_diff_volume, t_diff_marketcap
SELECT *
FROM t_crypto_metrics_1
	LEFT JOIN t_diff_volume
	USING (month)
	LEFT JOIN t_diff_marketcap
	USING (month)







