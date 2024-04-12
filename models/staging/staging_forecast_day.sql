WITH forecast_day_data AS (
    SELECT 
            (extracted_data -> 'forecast' -> 'forecastday' -> 0 ->> 'date')::DATE AS date
            ,(extracted_data ->'location'->>'name')::VARCHAR(255) AS city
            ,(extracted_data -> 'location' ->> 'region')::VARCHAR(255) AS region
            ,(extracted_data -> 'location' ->> 'country')::VARCHAR(255) AS country
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'maxtemp_c')::NUMERIC AS max_temp_c
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'mintemp_c')::NUMERIC AS min_temp_c
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avgtemp_c')::NUMERIC AS avg_temp_c
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'maxwind_kph')::NUMERIC AS max_wind_kph
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'totalprecip_mm')::NUMERIC AS total_precip_mm
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'totalsnow_cm')::NUMERIC AS total_snow_cm
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avgvis_km')::NUMERIC AS avg_vis_km
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avghumidity')::NUMERIC AS avg_humidity
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'daily_will_it_rain')::INT AS daily_will_it_rain
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'daily_chance_of_rain')::INT AS daily_chance_of_rain
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'daily_will_it_snow')::INT AS daily_will_it_snow
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'daily_chance_of_snow')::INT AS daily_chance_of_snow
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition' ->> 'text')::VARCHAR(255) AS condition_text
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition' ->> 'icon')::VARCHAR(255) AS condition_icon
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition' ->> 'code')::VARCHAR(255) AS condition_code
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'uv')::NUMERIC AS uv
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'sunrise') AS sunrise
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'sunset') AS sunset
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'moonrise') AS moonrise
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'moonset') AS moonset
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'moon_phase')::VARCHAR(255) AS moon_phase
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'moon_illumination')::NUMERIC AS moon_illumination
    FROM {{source("staging", "weather_raw")}})
SELECT * 
FROM forecast_day_data