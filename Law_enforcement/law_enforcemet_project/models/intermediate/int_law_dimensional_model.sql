-- dbt model for dimension
with source as (

    select * from {{ ref('stg_law_enforcement') }}

)
select
    id,
    cad_number,
    call_type_original,
    call_type_original_desc,
    call_type_final,
    call_type_final_desc,
    priority_orginal,
    priority_final,
    agency,
    onview_flag,
    intersection_name,
    intersection_id,
    intersection_point,
    supervisor_district,
    analysis_neighborhood,
    police_district,
    call_type_final_notes,
    call_type_original_notes
from source