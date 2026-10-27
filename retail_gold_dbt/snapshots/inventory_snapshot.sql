
{% snapshot inventory_snapshot %}

{{
    config(
        target_database='RETAIL_CLICKSTREAM_DB',
        target_schema='SNAPSHOTS',
        unique_key='PRODUCT_ID',
        strategy='check',
        check_cols=[
            'STOCK_QUANTITY',
            'REORDER_LEVEL',
            'INVENTORY_ORIGIN',
            'IS_OUT_OF_STOCK',
            'IS_AT_OR_BELOW_REORDER_LEVEL',
            'HAS_INVALID_STOCK_QUANTITY',
            'HAS_INVALID_REORDER_LEVEL',
            'IS_MISSING_INVENTORY_ORIGIN'
        ]
    )
}}

SELECT
    PRODUCT_ID,
    STOCK_QUANTITY,
    REORDER_LEVEL,
    INVENTORY_ORIGIN,
    IS_OUT_OF_STOCK,
    IS_AT_OR_BELOW_REORDER_LEVEL,
    HAS_INVALID_STOCK_QUANTITY,
    HAS_INVALID_REORDER_LEVEL,
    IS_MISSING_INVENTORY_ORIGIN
FROM {{ source('retail_staging', 'STG_INVENTORY') }}

{% endsnapshot %}
