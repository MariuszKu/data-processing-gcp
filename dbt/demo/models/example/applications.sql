
    select 
    ROW_NUMBER() OVER() AS application_id,
    b.client_id,
    c.product_id,  
    a.application_number,							
    a.application_date,				
    a.status,				
    a.credit_card_type,				
    cast(a.agreement_assignment_date as date) agreement_assignment_date,				
    cast(a.decision_date as date) decision_date	,			
    a.branch
    from
    `demodevraw.client_applications` a 
    LEFT JOIN {{ref('client')}} b on a.client_number = b.client_number
    LEFT JOIN {{ref('product')}} c on a.credit_card_type = c.credit_card_type

