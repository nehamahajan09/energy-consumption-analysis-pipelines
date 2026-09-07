-- ============================================================
-- SILVER LAYER - GRID LOAD
-- ============================================================
-- Source:
-- azuredb_cen.bronze.grid_load_stream
--
-- Transformations:
-- 1. Remove duplicate records
-- 2. Handle NULL/missing grid measurements
-- 3. Trim and standardize grid attributes
-- 4. Convert grid metrics to numeric types
-- 5. Validate load, capacity and measurement values
-- 6. Validate percentage-based metrics
-- ============================================================

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'grid_load_stream') }}

),

cleaned_data AS (

    SELECT DISTINCT

        -- ====================================================
        -- Grid attributes
        -- ====================================================
        TRIM(household_id) AS household_id,
        TRIM(grid_region) AS grid_region,
        TRIM(substation_name) AS substation_name,
        TRIM(feeder_line) AS feeder_line,
        TRIM(distribution_zone) AS distribution_zone,
        TRIM(grid_operator) AS grid_operator,

        -- ====================================================
        -- Numeric grid measurements
        -- Convert empty strings to NULL before casting
        -- ====================================================

        CAST(NULLIF(TRIM(grid_voltage), '') AS DOUBLE)
            AS grid_voltage,

        CAST(NULLIF(TRIM(grid_current), '') AS DOUBLE)
            AS grid_current,

        CAST(NULLIF(TRIM(grid_load_kw), '') AS DOUBLE)
            AS grid_load_kw,

        CAST(NULLIF(TRIM(transformer_load), '') AS DOUBLE)
            AS transformer_load,

        CAST(NULLIF(TRIM(line_loss_percent), '') AS DOUBLE)
            AS line_loss_percent,

        CAST(NULLIF(TRIM(load_variation), '') AS DOUBLE)
            AS load_variation,

        CAST(NULLIF(TRIM(frequency_variation), '') AS DOUBLE)
            AS frequency_variation,

        CAST(NULLIF(TRIM(grid_capacity_kw), '') AS DOUBLE)
            AS grid_capacity_kw,

        CAST(NULLIF(TRIM(demand_forecast_kw), '') AS DOUBLE)
            AS demand_forecast_kw,

        CAST(NULLIF(TRIM(reserve_margin), '') AS DOUBLE)
            AS reserve_margin

    FROM source_data

),

validated_data AS (

    SELECT

        -- ====================================================
        -- Grid attributes
        -- ====================================================

        household_id,
        grid_region,
        substation_name,
        feeder_line,
        distribution_zone,
        grid_operator,

        -- ====================================================
        -- Validate measurements
        -- Rule: must be >= 0
        -- ====================================================

        CASE
            WHEN grid_voltage >= 0
            THEN grid_voltage
            ELSE NULL
        END AS grid_voltage,

        CASE
            WHEN grid_current >= 0
            THEN grid_current
            ELSE NULL
        END AS grid_current,

        CASE
            WHEN grid_load_kw >= 0
            THEN grid_load_kw
            ELSE NULL
        END AS grid_load_kw,

        CASE
            WHEN transformer_load >= 0
            THEN transformer_load
            ELSE NULL
        END AS transformer_load,

        -- ====================================================
        -- Percentage-based metric
        -- Rule: between 0 and 100
        -- ====================================================

        CASE
            WHEN line_loss_percent BETWEEN 0 AND 100
            THEN line_loss_percent
            ELSE NULL
        END AS line_loss_percent,

        -- ====================================================
        -- Other grid measurements
        -- ====================================================

        CASE
            WHEN load_variation >= 0
            THEN load_variation
            ELSE NULL
        END AS load_variation,

        CASE
            WHEN frequency_variation >= 0
            THEN frequency_variation
            ELSE NULL
        END AS frequency_variation,

        -- Grid capacity must be > 0
        CASE
            WHEN grid_capacity_kw > 0
            THEN grid_capacity_kw
            ELSE NULL
        END AS grid_capacity_kw,

        -- Demand forecast must be >= 0
        CASE
            WHEN demand_forecast_kw >= 0
            THEN demand_forecast_kw
            ELSE NULL
        END AS demand_forecast_kw,

        CASE
            WHEN reserve_margin >= 0
            THEN reserve_margin
            ELSE NULL
        END AS reserve_margin

    FROM cleaned_data

)

SELECT *
FROM validated_data