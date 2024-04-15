WITH joining_day_location AS (
        SELECT * FROM {{ref('prep_forecast_day')}}
        LEFT JOIN {{ref('staging_location')}}
        USING(city,region,country)
),
filtering_features AS (
        SELECT 
            year_and_week
            ,week_of_year
            ,year
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
            -- ,condition_icon
            -- ,condition_code
            -- ,max_wind_kph
            -- ,avg_vis_km
            -- ,uv
            ,sunrise
            ,sunset
            -- ,moonrise
            -- ,moonset
            -- ,moon_phase
            -- ,moon_illumination
            -- ,day_of_month
            ,month_of_year
            -- ,day_of_week
        FROM joining_day_location
),          
aggregations_adding_features AS (
        SELECT 
            year_and_week  -- grouping on
            ,week_of_year   -- grouping on
            ,year           -- grouping on
            ,city           -- grouping on
            ,region         -- grouping on
            ,country        -- grouping on
            ,lat            -- grouping on
            ,lon            -- grouping on
            ,timezone_id    -- grouping on
            ,MAX(max_temp_c) AS max_temp_c
            ,MIN(min_temp_c) AS min_temp_c
            ,AVG(avg_temp_c) AS avg_temp_c
            ,SUM(total_precip_mm) AS total_precip_mm
            ,SUM(total_snow_cm) AS total_snow_cm
            ,AVG(avg_humidity) AS avg_humidity
            ,SUM(daily_will_it_rain) AS will_it_rain_days
            ,AVG(daily_chance_of_rain) AS daily_chance_of_rain_avg
            ,SUM(daily_will_it_snow) AS will_it_snow_days
            ,AVG(daily_chance_of_snow) AS daily_chance_of_snow_avg
            		,sum(case when condition_text = 'Sunny' then 1 else 0 end) as sunny_days
		,sum(case when condition_text in ('Cloudy','Partly cloudy','Overcast') then 1 else 0 end) as cloudy_days
		,sum(case when condition_text in ('Fog','Mist') then 1 else 0 end) as mystical_days
		,sum(case when condition_text in ('Heavy rain at times'
                                    ,'Moderate rain'
                                    ,'Patchy rain possible'
                                    ,'Light drizzle'
                                    ,'Heavy rain'
                                    ,'Moderate or heavy rain shower'
                                    ,'Light rain'
                                    ,'Patchy light drizzle'
                                    ,'Light freezing rain'
                                    ,'Patchy light rain with thunder'
                                    ,'Moderate or heavy rain with thunder'
                                    ,'Light rain shower'
                                    ,'Moderate rain at times') then 1 else 0 end) as rainy_days
		,sum(case when condition_text in ('Moderate snow'
									,'Moderate or heavy snow with thunder'
									,'Light snow'
									,'Moderate or heavy snow showers'
									,'Light sleet'
									,'Patchy snow possible'
									,'Light snow showers'
									,'Heavy snow'
									,'Blowing snow'
									,'Patchy light snow with thunder'
									,'Moderate or heavy sleet'
									,'Light sleet showers'
									,'Patchy light snow') then 1 else 0 end) as snow_days
		,sum(case when condition_text in ('Blizzard') then 1 else 0 end) as stay_at_home_days
    FROM filtering_features
    where city = 'Glasgow'
    GROUP BY (year_and_week, month_of_year, week_of_year, year, city, region, country, lat, lon, timezone_id)
    ORDER BY city
)
SELECT * 
FROM aggregations_adding_features
where city = 'Glasgow'