WITH forecast_hour_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_hour')}}
),
add_features AS (
    SELECT *
        ,date_time::time AS time -- only time (hours:minutes:seconds) as TIME data type
        ,TO_CHAR(date_time, 'HH24:MI') AS hour -- time (hours:minutes) as TEXT data type
        ,to_char(date_time,'month') AS month_of_year -- month name as a text
        ,to_char(date_time, 'day') AS day_of_week -- weekday name as text
    FROM forecast_hour_data
)
SELECT *
FROM add_features