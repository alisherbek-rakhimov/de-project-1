DROP TABLE if exists analysis.dm_rfm_segments;

create table analysis.dm_rfm_segments
(
    user_id        int primary key,
    recency        int check ( recency in (1, 2, 3, 4, 5) ),
    frequency      int check ( frequency in (1, 2, 3, 4, 5) ),
    monetary_value int check ( monetary_value in (1, 2, 3, 4, 5) )
);

insert into analysis.dm_rfm_segments
with gte_2021_closed_orders as (select o.order_id, user_id, payment, order_ts
                                from analysis.orderstatuslog ol
                                         join analysis.orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     tmp as (select distinct u.id                                                       as user_id,
                             coalesce(extract(epoch from max(order_ts) over (partition by u.id)), 0) as last_order_was,
                             count(order_id) over (partition by u.id)                   as orders_cnt,
                             coalesce(sum(payment) over (partition by u.id), 0)         as spent
             from analysis.users u
                      left join gte_2021_closed_orders on u.id = gte_2021_closed_orders.user_id)
select user_id,
--        last_order_was,
       ntile(5) over (order by last_order_was) as recency,
--        orders_cnt,
       ntile(5) over (order by orders_cnt )         as frequency,
--        spent,
       ntile(5) over (order by spent)               as monetary_value
from tmp
