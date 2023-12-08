WITH join_time_with_date AS (
    SELECT
		year,
		TO_TIMESTAMP(CONCAT(year, '-', LPAD(month::text, 2, '0'), '-', LPAD(day::text, 2, '0'), '-', timestamp), 'YYYY-MM-DD-HH24:MI:SS') AS date_time
	FROM
		dim_date_times
),
next_time_date_with_date AS (
	SELECT
		year,
		date_time,
		LEAD(date_time, 1) OVER (PARTITION BY year ORDER BY date_time) AS next_time_date
	FROM
		join_time_with_date
),
avg_diff AS(
	SELECT
		year,
		AVG(next_time_date - date_time) AS avg_time_diff
	FROM
		next_time_date_with_date
	GROUP BY
		year
)
SELECT
	year,
	CONCAT(
        '"hours":', EXTRACT(HOUR FROM avg_time_diff),
        ', "minutes":', EXTRACT(MINUTE FROM avg_time_diff),
        ', "seconds":', EXTRACT(SECOND FROM avg_time_diff),
        ', "milliseconds":', ROUND(EXTRACT(MILLISECONDS FROM avg_time_diff)),
        '"'
    ) AS actual_time_taken
	FROM avg_diff
ORDER BY
	avg_time_diff DESC
LIMIT
	5;