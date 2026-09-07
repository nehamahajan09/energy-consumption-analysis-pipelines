-- ============================================================
-- SILVER LAYER - TARIFF
-- ============================================================

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'tariff_metrics_stream_v2') }}

),

cleaned_data AS (

    SELECT DISTINCT

        -- Categorical columns
        TRIM(tariff_region) AS tariff_region,
        TRIM(tariff_city) AS tariff_city,
        TRIM(tariff_plan_type) AS tariff_plan_type,
        TRIM(billing_cycle) AS billing_cycle,
        TRIM(utility_provider) AS utility_provider,

        -- Numeric columns
        TRY_CAST(TRIM(NULLIF(unit_rate, '')) AS DOUBLE) AS unit_rate,
        TRY_CAST(TRIM(NULLIF(peak_rate, '')) AS DOUBLE) AS peak_rate,
        TRY_CAST(TRIM(NULLIF(offpeak_rate, '')) AS DOUBLE) AS offpeak_rate,
        TRY_CAST(TRIM(NULLIF(fixed_charge, '')) AS DOUBLE) AS fixed_charge,
        TRY_CAST(TRIM(NULLIF(tax_amount, '')) AS DOUBLE) AS tax_amount,
        TRY_CAST(TRIM(NULLIF(subsidy_amount, '')) AS DOUBLE) AS subsidy_amount,
        TRY_CAST(TRIM(NULLIF(monthly_bill, '')) AS DOUBLE) AS monthly_bill,
        TRY_CAST(TRIM(NULLIF(billing_units, '')) AS DOUBLE) AS billing_units,
        TRY_CAST(TRIM(NULLIF(late_fee, '')) AS DOUBLE) AS late_fee,
        TRY_CAST(TRIM(NULLIF(adjustment_amount, '')) AS DOUBLE) AS adjustment_amount

    FROM source_data

),

validated_data AS (

    SELECT

        tariff_region,
        tariff_city,
        tariff_plan_type,
        billing_cycle,
        utility_provider,

        -- Values must be >= 0
        CASE
            WHEN unit_rate >= 0 THEN unit_rate
            ELSE NULL
        END AS unit_rate,

        CASE
            WHEN peak_rate >= 0 THEN peak_rate
            ELSE NULL
        END AS peak_rate,

        CASE
            WHEN offpeak_rate >= 0 THEN offpeak_rate
            ELSE NULL
        END AS offpeak_rate,

        CASE
            WHEN fixed_charge >= 0 THEN fixed_charge
            ELSE NULL
        END AS fixed_charge,

        tax_amount,

        -- Negative values are valid
        subsidy_amount,

        CASE
            WHEN monthly_bill >= 0 THEN monthly_bill
            ELSE NULL
        END AS monthly_bill,

        CASE
            WHEN billing_units >= 0 THEN billing_units
            ELSE NULL
        END AS billing_units,

        CASE
            WHEN late_fee >= 0 THEN late_fee
            ELSE NULL
        END AS late_fee,

        -- Negative values are valid
        adjustment_amount

    FROM cleaned_data

)

SELECT *
FROM validated_data