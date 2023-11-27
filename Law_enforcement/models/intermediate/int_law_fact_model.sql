-- dbt model for fact (time-related)
with source as (

    select * from {{ ref('stg_law_enforcement') }}

)

select
    id,
    received_datetime,
    entry_datetime,
    dispatch_datetime,
    enroute_datetime,
    onscene_datetime,
    close_datetime,
    call_last_updated_at,
    data_as_of,
    data_loaded_at
from source
