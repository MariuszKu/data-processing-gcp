
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}
with region as (
SELECT 
	ROW_NUMBER() OVER() AS region_id,
  city,
  state,
  postcode
FROM 
`project.demodevraw.clients` 


)


select 
*
from 
region


