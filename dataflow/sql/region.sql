create or replace table {{ params.project_id }}.{{ params.dest_dataset }}.df_region as (
SELECT 
	ROW_NUMBER() OVER() AS region_id,
  city,
  state,
  postcode
FROM 
`{{ params.project_id }}.{{ params.dest_dataset }}` 