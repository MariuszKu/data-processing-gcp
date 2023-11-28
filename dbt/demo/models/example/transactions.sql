select 
ROW_NUMBER() OVER() AS transaction_id,
b.client_id,
c.product_id,   
d.merchant_id,
a.transaction_id as bk_transaction_id,
a.client_num,
cast(a.transaction_amount as decimal) transaction_amount,
cast(a.transaction_date as date) transaction_date,
a.status,

from
`demodevraw.card_operations` a 
LEFT JOIN  {{ref('client')}} b on a.client_num = b.client_number
LEFT JOIN {{ref('applications')}} c on b.client_id = c.client_id
LEFT JOIN {{ref('merchant')}} d on a.merchant = d.merchant