{{ config(
    materialized='table',
    schema='silver'
) }}

-- ============================================================
-- SILVER LAYER - ENERGY METRICS
-- ============================================================
-- Source:
-- azuredb_cen.bronze.energy_usage_stream
--
-- Transformations:
-- 1. Remove duplicate records
-- 2. Handle NULL / missing values
-- 3. Trim leading/trailing spaces
-- 4. Standardize categorical values
-- 5. Convert numeric columns to DOUBLE
-- 6. Convert timestamp to TIMESTAMP
-- 7. Derive date, year, month, day, hour, day-of-week
-- 8. Validate energy and demand measurements
-- ============================================================

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'energy_usage_stream') }}

),

cleaned_data AS (

    SELECT DISTINCT

        -- Identifier
        TRIM(household_id) AS household_id,

        -- Categorical columns
        INITCAP(TRIM(region_name)) AS region_name,
        INITCAP(TRIM(city_name)) AS city_name,
        INITCAP(TRIM(meter_type)) AS meter_type,
        INITCAP(TRIM(customer_category)) AS customer_category,
        INITCAP(TRIM(grid_zone)) AS grid_zone,

        -- Numeric measurements
        TRY_CAST(NULLIF(TRIM(voltage_reading), '') AS DOUBLE)
            AS voltage_reading,

        TRY_CAST(NULLIF(TRIM(current_reading), '') AS DOUBLE)
            AS current_reading,

        TRY_CAST(NULLIF(TRIM(active_power_kw), '') AS DOUBLE)
            AS active_power_kw,

        TRY_CAST(NULLIF(TRIM(reactive_power_kvar), '') AS DOUBLE)
            AS reactive_power_kvar,

        TRY_CAST(NULLIF(TRIM(energy_usage_kwh), '') AS DOUBLE)
            AS energy_usage_kwh,

        TRY_CAST(NULLIF(TRIM(frequency_hz), '') AS DOUBLE)
            AS frequency_hz,

        TRY_CAST(NULLIF(TRIM(load_factor), '') AS DOUBLE)
            AS load_factor,

        TRY_CAST(NULLIF(TRIM(peak_demand_kw), '') AS DOUBLE)
            AS peak_demand_kw,

        TRY_CAST(NULLIF(TRIM(offpeak_demand_kw), '') AS DOUBLE)
            AS offpeak_demand_kw,

        TRY_CAST(NULLIF(TRIM(daily_consumption_kwh), '') AS DOUBLE)
            AS daily_consumption_kwh,

        -- Timestamp
        TRY_CAST(timestamp AS TIMESTAMP) AS timestamp

    FROM source_data

),

validated_data AS (

    SELECT *

    FROM cleaned_data

    WHERE
        -- Energy and demand must be >= 0
        (voltage_reading IS NULL OR voltage_reading >= 0)

        AND (current_reading IS NULL OR current_reading >= 0)

        AND (active_power_kw IS NULL OR active_power_kw >= 0)

        AND (reactive_power_kvar IS NULL OR reactive_power_kvar >= 0)

        AND (energy_usage_kwh IS NULL OR energy_usage_kwh >= 0)

        AND (frequency_hz IS NULL OR frequency_hz >= 0)

        AND (peak_demand_kw IS NULL OR peak_demand_kw >= 0)

        AND (offpeak_demand_kw IS NULL OR offpeak_demand_kw >= 0)

        AND (daily_consumption_kwh IS NULL OR daily_consumption_kwh >= 0)

        -- Load factor expected range: 0 to 1
        AND (
            load_factor IS NULL
            OR load_factor BETWEEN 0 AND 1
        )

)

SELECT

    household_id,
    region_name,
    city_name,
    meter_type,
    customer_category,
    grid_zone,

    voltage_reading,
    current_reading,
    active_power_kw,
    reactive_power_kvar,
    energy_usage_kwh,
    frequency_hz,
    load_factor,
    peak_demand_kw,
    offpeak_demand_kw,
    daily_consumption_kwh,

    timestamp,

    -- Derived date/time attributes
    CAST(timestamp AS DATE) AS usage_date,
    YEAR(timestamp) AS usage_year,
    MONTH(timestamp) AS usage_month,
    DAY(timestamp) AS usage_day,
    HOUR(timestamp) AS usage_hour,
    DAYOFWEEK(timestamp) AS day_of_week

FROM validated_data