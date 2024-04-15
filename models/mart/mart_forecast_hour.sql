WITH joining_hour_location AS (
        SELECT * FROM {{ref('prep_forecast_hour')}}
        LEFT JOIN {{ref('staging_location')}}
        USING(city,region,country)
),
adding_features AS (
        SELECT 
            *,
            CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') AS condition_icon_md
        FROM joining_hour_location
),
filtering_ordering_features AS (
        SELECT 
            date
            ,city
            ,region
            ,country
            ,time_epoch
            ,date_time
            ,is_day
            ,time
            ,hour
            ,month_of_year
            ,day_of_week
            ,condition_text
            ,condition_icon
            ,condition_icon_md
            ,condition_code
            ,temp_c
            ,wind_kph
            ,wind_degree
            ,wind_dir
            ,pressure_mb
            ,precip_mm
            ,snow_cm
            ,humidity
            ,cloud
            ,feelslike_c
            ,windchill_c
            ,heatindex_c
            ,dewpoint_c
            ,will_it_rain
            ,chance_of_rain
            ,will_it_snow
            ,chance_of_snow
            ,vis_km
            ,gust_kph
            ,uv
            FROM adding_features
)
SELECT * 
FROM filtering_ordering_features