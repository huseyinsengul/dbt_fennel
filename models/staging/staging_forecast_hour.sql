WITH forecast_hour_raw AS (
    SELECT 
            (extracted_data -> 'forecast' -> 'forecastday' -> 0 ->> 'date')::DATE AS date
            ,(extracted_data ->'location'->>'name')::VARCHAR(255) AS city
            ,(extracted_data -> 'location' ->> 'region')::VARCHAR(255) AS region
            ,(extracted_data -> 'location' ->> 'country')::VARCHAR(255) AS country
            ,JSON_ARRAY_ELEMENTS(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour') AS hour_data
    FROM {{source("staging", "weather_raw")}}
),
forecast_hour_data AS (
    SELECT 
            date
            ,city
            ,region
            ,country		
            ,(hour_data ->> 'time_epoch')::INTEGER AS time_epoch
            ,(hour_data ->> 'time')::TIMESTAMP AS date_time
            ,(hour_data ->> 'is_day')::INTEGER AS is_day			
            ,(hour_data -> 'condition' ->> 'text')::VARCHAR(255) AS condition_text
            ,(hour_data -> 'condition' ->> 'icon')::VARCHAR(255) AS condition_icon
            ,(hour_data -> 'condition' ->> 'code')::INTEGER AS condition_code
            ,(hour_data ->> 'temp_c')::NUMERIC AS temp_c
            ,(hour_data ->> 'wind_kph')::NUMERIC AS wind_kph
            ,(hour_data ->> 'wind_degree')::INTEGER AS wind_degree
            ,(hour_data ->> 'wind_dir')::VARCHAR(255) AS wind_dir
            ,(hour_data ->> 'pressure_mb')::NUMERIC AS pressure_mb
            ,(hour_data ->> 'precip_mm')::NUMERIC AS precip_mm
            ,(hour_data ->> 'snow_cm')::NUMERIC AS snow_cm
            ,(hour_data ->> 'humidity')::INTEGER AS humidity
            ,(hour_data ->> 'cloud')::INTEGER AS cloud
            ,(hour_data ->> 'feelslike_c')::NUMERIC AS feelslike_c
            ,(hour_data ->> 'windchill_c')::NUMERIC AS windchill_c
            ,(hour_data ->> 'heatindex_c')::NUMERIC AS heatindex_c
            ,(hour_data ->> 'dewpoint_c')::NUMERIC AS dewpoint_c
            ,(hour_data ->> 'will_it_rain')::INTEGER AS will_it_rain
            ,(hour_data ->> 'chance_of_rain')::NUMERIC AS chance_of_rain
            ,(hour_data ->> 'will_it_snow')::INTEGER AS will_it_snow
            ,(hour_data ->> 'chance_of_snow')::NUMERIC AS chance_of_snow
            ,(hour_data ->> 'vis_km')::NUMERIC AS vis_km
            ,(hour_data ->> 'gust_kph')::NUMERIC AS gust_kph
            ,(hour_data ->> 'uv')::NUMERIC AS uv
    FROM forecast_hour_raw
    )
SELECT * 
FROM forecast_hour_data