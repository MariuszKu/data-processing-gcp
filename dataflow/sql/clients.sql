create or replace table  {{ params.project_id }}.{{ params.dest_dataset }}.df_clients
select 
ROW_NUMBER() OVER() AS client_id,
b.region_id, 
client_number,
Upper(name) name,				
email,			
phone_number,
bulding_number,
street_name,
cast(birth_date as date) birth_date

from
`{{ params.project_id }}.{{ params.dest_dataset }}.clients` a 
LEFT JOIN {{ params.project_id }}.{{ params.dest_dataset }}.region b on a.postcode = b.postcode
and a.city = b.city
and a.state = b.state