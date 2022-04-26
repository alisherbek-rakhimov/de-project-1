DROP TABLE if exists dm_rfm_segments;

create table dm_rfm_segments
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
     gte_2021_closed_orders_with_no_order_users as (
         -- Тут нужно отобрать именно юзеров с успешными заказами и тех которые вообще ничего не закали
         -- и нужно исключить тех кто заказал но еще не Closed для расчета RFM
         select o.order_id, u.id as user_id, payment, order_ts
         from analysis.users u
                  left join analysis.orders o on u.id = o.user_id
         where order_id is null
         union all
         select *
         from gte_2021_closed_orders),
     u_recency as (select user_id as user_id, max(order_ts) last_order_was
                   from gte_2021_closed_orders_with_no_order_users o
                   group by user_id),
     u_frequency as (select user_id, count(order_id) orders_cnt
                     from gte_2021_closed_orders_with_no_order_users o
                     group by user_id),
     u_monetary as (select user_id as user_id, sum(payment) spent
                    from gte_2021_closed_orders_with_no_order_users o
                    group by user_id)
select ur.user_id,
--        spent,
       ntile(5) over (order by spent)               as monetary_value,
--        last_order_was,
       ntile(5) over (order by last_order_was desc) as recency,
--        orders_cnt,
       ntile(5) over (order by orders_cnt)          as frequency
from u_recency ur
         join u_frequency uf on ur.user_id = uf.user_id
         join u_monetary um on ur.user_id = um.user_id;