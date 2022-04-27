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
     u_recency as (select u.id as user_id, max(order_ts) last_order_was
                   from gte_2021_closed_orders o
                            right join analysis.users u on o.user_id = u.id
                   group by u.id),
     u_frequency as (select u.id as user_id, count(order_id) orders_cnt
                     from gte_2021_closed_orders o
                              join analysis.users u on o.user_id = u.id
                     group by u.id),
     u_monetary as (select u.id as user_id, sum(payment) spent
                    from gte_2021_closed_orders o
                             join analysis.users u on o.user_id = u.id
                    group by u.id)
select u_recency.user_id,
--        last_order_was,
       ntile(5) over (order by last_order_was desc) as recency,
--        orders_cnt,
       ntile(5) over (order by orders_cnt)          as frequency,
--        spent,
       ntile(5) over (order by spent)               as monetary_value
from u_recency
       left join u_frequency on u_recency.user_id = u_frequency.user_id
       left join u_monetary on u_recency.user_id = u_monetary.user_id