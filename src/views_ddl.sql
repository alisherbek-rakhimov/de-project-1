create view analysis.users as
select *
from production.users;

create view analysis.orders as
select *
from production.orders;

create view analysis.orderstatuses as
select *
from production.orderstatuses;

create view analysis.products as
select *
from production.products;

create view analysis.orderstatuslog as
select *
from production.orderstatuslog;

create view analysis.orderitems as
select *
from production.orderitems;