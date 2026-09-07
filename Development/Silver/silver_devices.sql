-- ============================================================
-- SILVER LAYER - DEVICE METRICS
-- ============================================================
-- Transformations:
-- 1. Remove duplicate records
-- 2. Handle NULL/missing device measurements
-- 3. Trim and standardize device attributes
-- 4. Convert device metrics to numeric types
-- 5. Validate device measurements
-- 6. Validate efficiency values
-- 7. Remove records with NULL critical measurements
-- ============================================================

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'device_metrics_stream') }}

),

cleaned_data AS (

    SELECT DISTINCT

        -- Device attributes
        TRIM(device_category) AS device_category,
        TRIM(device_brand) AS device_brand,
        TRIM(device_model) AS device_model,
        TRIM(maintenance_status) AS maintenance_status,
        TRIM(installation_region) AS installation_region,

        -- Numeric measurements
        TRY_CAST(NULLIF(TRIM(runtime_hours), '') AS DOUBLE) AS runtime_hours,
        TRY_CAST(NULLIF(TRIM(device_power_kw), '') AS DOUBLE) AS device_power_kw,
        TRY_CAST(NULLIF(TRIM(motor_speed_rpm), '') AS DOUBLE) AS motor_speed_rpm,
        TRY_CAST(NULLIF(TRIM(efficiency_ratio), '') AS DOUBLE) AS efficiency_ratio,
        TRY_CAST(NULLIF(TRIM(energy_draw_kwh), '') AS DOUBLE) AS energy_draw_kwh,
        TRY_CAST(NULLIF(TRIM(heat_output), '') AS DOUBLE) AS heat_output,
        TRY_CAST(NULLIF(TRIM(cooling_load), '') AS DOUBLE) AS cooling_load,
        TRY_CAST(NULLIF(TRIM(device_voltage), '') AS DOUBLE) AS device_voltage,
        TRY_CAST(NULLIF(TRIM(device_current), '') AS DOUBLE) AS device_current,
        TRY_CAST(NULLIF(TRIM(device_temperature), '') AS DOUBLE) AS device_temperature

    FROM source_data

),

validated_data AS (

    SELECT

        device_category,
        device_brand,
        device_model,
        maintenance_status,
        installation_region,

        -- Runtime validation
        CASE
            WHEN runtime_hours >= 0 THEN runtime_hours
            ELSE NULL
        END AS runtime_hours,

        -- Power validation
        CASE
            WHEN device_power_kw >= 0 THEN device_power_kw
            ELSE NULL
        END AS device_power_kw,

        -- Motor speed validation
        CASE
            WHEN motor_speed_rpm >= 0 THEN motor_speed_rpm
            ELSE NULL
        END AS motor_speed_rpm,

        -- Efficiency validation
        CASE
            WHEN efficiency_ratio BETWEEN 0 AND 100
            THEN efficiency_ratio
            ELSE NULL
        END AS efficiency_ratio,

        -- Energy validation
        CASE
            WHEN energy_draw_kwh >= 0 THEN energy_draw_kwh
            ELSE NULL
        END AS energy_draw_kwh,

        -- Heat output validation
        CASE
            WHEN heat_output >= 0 THEN heat_output
            ELSE NULL
        END AS heat_output,

        -- Cooling load validation
        CASE
            WHEN cooling_load >= 0 THEN cooling_load
            ELSE NULL
        END AS cooling_load,

        -- Voltage validation
        CASE
            WHEN device_voltage >= 0 THEN device_voltage
            ELSE NULL
        END AS device_voltage,

        -- Current validation
        CASE
            WHEN device_current >= 0 THEN device_current
            ELSE NULL
        END AS device_current,

        -- Temperature validation
        CASE
            WHEN device_temperature >= 0 THEN device_temperature
            ELSE NULL
        END AS device_temperature

    FROM cleaned_data

),

final_data AS (

    SELECT *
    FROM validated_data

    -- Critical fields must not be NULL
    WHERE energy_draw_kwh IS NOT NULL
      AND device_power_kw IS NOT NULL

)

SELECT *
FROM final_data