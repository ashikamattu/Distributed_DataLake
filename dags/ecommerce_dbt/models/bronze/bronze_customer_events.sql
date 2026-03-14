{{ config(materialized='table', tags=['bronze']) }}

select
    event_id,
    customer_id,
    session_id,
    event_type,
    cast(event_timestamp as timestamp) as event_timestamp,
    page_url,
    coalesce(product_id, '') as product_id,
    coalesce(category_id, '') as category_id,
    referrer_source,
    device_type,
    user_agent,
    ip_address,
    CURRENT_TIMESTAMP as ingestion_timestamp,
    'RAW_CUSTOMER_EVENTS' as source_system  
from {{ ref('raw_customer_events') }}