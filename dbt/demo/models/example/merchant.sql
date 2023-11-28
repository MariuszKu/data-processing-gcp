with dane as (
    select distinct
    merchant
    from
    `demodevraw.card_operations`

)

select 
ROW_NUMBER() OVER() AS merchant_id,
merchant
from
dane

