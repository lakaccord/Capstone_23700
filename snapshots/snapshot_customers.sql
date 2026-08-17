{% snapshot CUSTOMERS_SNAPSHOT %}
 
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}
 
WITH flattened AS (
 
    SELECT
        customer.value:customer_id::VARCHAR AS customer_id,
 
        TRY_TO_TIMESTAMP_NTZ(
            customer.value:last_modified_date::STRING
        ) AS last_modified_date,
 
        customer.value AS raw_customer_data,
 
        b.SOURCE_FILE,
        b.LOADED_AT,
        b.BATCH_ID
 
    FROM {{ ref('bronze_customers') }} AS b,
 
    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:customers_data
    ) AS customer
 
),
 
latest_customer AS (
 
    SELECT
        customer_id,
        last_modified_date,
        raw_customer_data,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID
 
    FROM flattened
 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY
            last_modified_date DESC,
            LOADED_AT DESC,
            SOURCE_FILE DESC
    ) = 1
 
)
 
SELECT
    customer_id,
    last_modified_date,
    raw_customer_data,
    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID
 
FROM latest_customer
 
{% endsnapshot %}