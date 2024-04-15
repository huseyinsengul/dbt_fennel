WITH joining_day_location AS (
        SELECT * FROM {{ref('prep_forecast_day')}}
        LEFT JOIN {{ref('staging_location')}}
        USING(city,region,country)
),
adding_features AS (
        SELECT 
            *
            ,CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') AS condition_icon_md
            ,(CASE WHEN moonrise = 'No moonrise' THEN null ELSE moonrise END)::TIME AS moonrise_n
            ,(CASE WHEN moonset = 'No moonset' THEN null ELSE moonset END)::TIME AS moonset_n
            ,(CASE WHEN sunrise = 'No sunrise' THEN null ELSE sunrise END)::TIME AS sunrise_n
            ,(CASE WHEN sunset = 'No sunset' THEN null ELSE sunset END)::TIME AS sunset_n
        FROM joining_day_location
),
filtering_ordering_features AS (
        SELECT 
            date
            ,day_of_month
            ,month_of_year
            ,year
            ,day_of_week
            ,week_of_year
            ,year_and_week
            ,city
            ,region
            ,country
            ,lat
            ,lon
            ,timezone_id
            ,max_temp_c
            ,min_temp_c
            ,avg_temp_c
            ,total_precip_mm
            ,total_snow_cm
            ,avg_humidity
            ,daily_will_it_rain
            ,daily_chance_of_rain
            ,daily_will_it_snow
            ,daily_chance_of_snow
            ,condition_text
            ,condition_icon
            ,condition_icon_md
            ,condition_code
            ,max_wind_kph
            ,avg_vis_km
            ,uv
            ,sunrise_n
            ,sunset_n
            ,moonrise_n
            ,moonset_n
            ,moon_phase
            ,moon_illumination
        FROM adding_features
)
SELECT * 
FROM filtering_ordering_features