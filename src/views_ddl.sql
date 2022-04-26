DROP view if exists analysis.users;
create view analysis.users as
select *
from production.users;

DROP view if exists analysis.users;
create view analysis.orders as
select *
from production.orders;

DROP view if exists analysis.users;
create view analysis.orderstatuses as
select *
from production.orderstatuses;

DROP view if exists analysis.users;
create view analysis.products as
select *
from production.products;

DROP view if exists analysis.users;
create view analysis.orderstatuslog as
select *
from production.orderstatuslog;

DROP view if exists analysis.users;
create view analysis.orderitems as
select *
from production.orderitems;