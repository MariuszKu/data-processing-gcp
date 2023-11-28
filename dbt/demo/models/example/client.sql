
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}


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
`project.demodevraw.clients` a LEFT JOIN {{ref('region')}} b on a.postcode = b.postcode
and a.city = b.city
and a.state = b.state

