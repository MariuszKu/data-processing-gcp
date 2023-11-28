
with product as (
    select distinct  
    credit_card_type
    from
    `demodevraw.client_applications`

)

select 
ROW_NUMBER() OVER() AS product_id,
credit_card_type

from
product