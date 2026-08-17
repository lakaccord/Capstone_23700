{{ config(
    materialized='view',
    schema='reporting'
) }}

SELECT

    fmp.campaign_key,
    dmc.campaign_id,

    dmc.target_audience_segment AS campaign_type,

    fmp.date_key,
    dd.full_date,

    fmp.total_sales_influenced,

    fmp.total_campaign_cost,

    fmp.roi

FROM {{ ref('Fact_Performance') }} fmp

LEFT JOIN {{ ref('Dim_Marketing_Campaign') }} dmc

    ON fmp.campaign_key =
       dmc.campaign_key

LEFT JOIN {{ ref('Dim_Date') }} dd

    ON fmp.date_key =
       dd.date_key