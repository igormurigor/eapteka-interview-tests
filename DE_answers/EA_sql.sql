
-- SQL 1
select *,
case 
	when  row_number() over(partition by client_id order by datetime desc) = 1
	then 1
	else 0
end 	
as  is_last_operation from public.sales s2 

-- SQL 2
-- DQ метрики
-- 1. Формат даты
-- 2.  проверка значения колонки return_flag (N/Y)

-- SQL 3.1
select tt.email from (
select *, row_number() over(partition by email) as email_count from public.client) as tt
where email_count = 2


-- SQL 3.2
select t1.id  
from client as t1
inner join (select id, email, row_number() over(partition by email) as email_count from client ) as t2
on t1.email = t2.email 
where t2.email_count = 2

