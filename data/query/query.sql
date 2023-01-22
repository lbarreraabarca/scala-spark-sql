select 
    t.id as ticket_id, 
    c.id as customer_id, 
    c.name as customer_name, 
    p.id as product_id, 
    p.name as product_name, 
    p.price as product_price,
    sum(p.price) over (partition by c.id) as customer_total_price
from tickets t 
inner join customer c on t.customer_id = c.id 
inner join product p on t.product_id = p.id