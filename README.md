# Проект 1

![production database er diagram](src/production.svg "production diagram")

## 1.2

<hr>

Посмотрев на ER диаграмму можно понять что типы статусов храняться в таблице
```orderstatuses``` и находим в нем какой id у ```Cancelled``` чтоб можно было
достать только успешные заказы.

| id | key |
|:----:| :--- |
| 1 | Open |
| 2 | Cooking |
| 3 | Delivering |
| 4 | Closed |
| 5 | Cancelled |

Как мы видим ```id=4``` это статус ```Closed```

## Изучите структуру исходных данных И Проанализируйте качество данных

<hr>
Начнем с проверки 
заказов. Будем иметь ввиду что заказ во время cooking и delivering
отменить нельзя. Получается заказ должен быть Open и только потом может быть Cancelled
И тут мы проверим нет ли таких заказов из за Багов в приложении

```postgresql
with orders_prev_stat as (select order_id,
                                 key                                                          status,
                                 dttm,
                                 lag(key, 1, null) over (partition by order_id order by dttm) prev_status
                          from orderstatuslog ol
                                   join orderstatuses o on ol.status_id = o.id
                          order by order_id)
select *
from orders_prev_stat
where status = 'Cancelled'
  and prev_status != 'Open'
```

| order\_id | status | dttm | prev\_status |
| :--- | :--- | :--- | :--- |

Видим что с заказами все в порядке.

<hr>

В таблице orders ```user_id``` не ```FK```. Это значит что в
базе может быть заказы без юзера, давайте проверим наличие
таких заказов

```postgresql
select order_id
from orders
where user_id not in (select user_id from users);
```

| order\_id |
| :--- |

Есть ли заказы юзеров которых нет в нашей таблице.
Через 2й запрос можно точно выявить поврежденные заказа
но как мы видим их нет. Но нужно исправить это чтоб ```user_id``` указал на ```users.user_id```

<hr>

Дальше можно проверить нет ли отрицательных значений в такиз полях
как bonus_payment/payment/cost/bonus_grant.

<hr>

### Про существующие ограничения

В ```orders``` есть ```constraint orders_check
check (cost = (payment + bonus_payment))``` так что
не придеться нам самим проверить

<hr>

Продукты не должны иметь отрицательные цены
```constraint products_price_check check (price >= (0)::numeric)```
<hr>

В ```orderstatuslog``` стоит
```unique (order_id, status_id)``` то есть
у заказа не может быть два одинаковых статуса одновременно

<hr>

И еще в ```orderitems``` можем увидеть что при добавлении одного и того же
товара оно не должно дублироваться а наобарот увеличить счетчик на единицу.
И как упомянулось ранее сумма их цен !< 0
Скидка больше 0 и не должна перевышать цену товара, так как ценнык не может иметь
отрацательное значение

```
price      numeric(19, 5) default 0 not null
        constraint orderitems_price_check
            check (price >= (0)::numeric),
    discount   numeric(19, 5) default 0 not null,
    quantity   integer                  not null
        constraint orderitems_quantity_check
            check (quantity > 0),
    unique (order_id, product_id),
    constraint orderitems_check
        check ((discount >= (0)::numeric) AND (discount <= price))
```

<hr>

Было бы хорошо если у юзера ```name``` был обязательным, то есть ```not null```

<hr>

# Подготовьте витрину данных

В файле ```views_ddl.sql``` скрипты для создания представлений для
каждой таблицы из ```production```

<hr>

Создадим витрину. Лучше сделать user_id сразу PK, так как он тут distinct

```postgresql
create table dm_rfm_segments
(
    user_id        int primary key,
    recency        int check ( recency in (1, 2, 3, 4, 5) ),
    frequency      int check ( frequency in (1, 2, 3, 4, 5) ),
    monetary_value int check ( monetary_value in (1, 2, 3, 4, 5) )
)
```

<hr>

## Как провести RFM-сегментацию

Сперва нужно достать из базы успешные(Closed) заказы с начала 2021 года
```postgresql
with gte_2021_closed_orders as (select o.order_id, user_id, payment, order_ts
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021)
```

И перед тем как мержить все в единый запрос давайте проверим что
количество клиентов в каждом сегменте одинаково. Тут я решил пользоваться
подзапросом ради чатаемости, так как оно кэшируется и не выполняется
при каждой итерации

```postgresql
with gte_2021_closed_orders as (select o.order_id
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     u_recency as (select user_id, now() - max(order_ts) last_order_was
                   from orders o
                            right join users u on o.user_id = u.id
                   where o.order_id in (select * from gte_2021_closed_orders)
                   group by user_id),
     tmp as (select user_id, last_order_was, ntile(5) over (order by last_order_was desc) recency
                     from u_recency
                     order by recency desc)
select recency, count(user_id)
from tmp
group by recency
```

| recency | count |
| :--- | :--- |
| 3 | 198 |
| 5 | 197 |
| 4 | 197 |
| 2 | 198 |
| 1 | 198 |

```postgresql
with gte_2021_closed_orders as (select o.order_id
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     u_monetary as (select user_id, sum(payment) spent
                    from orders o
                             join users u on o.user_id = u.id
                    where o.order_id in (select * from gte_2021_closed_orders)
                    group by user_id),
     tmp as (select user_id, spent, ntile(5) over (order by spent) monetary
                      from u_monetary)

select monetary, count(user_id)
from tmp
group by monetary
```

| monetary | count |
| :--- | :--- |
| 3 | 198 |
| 5 | 197 |
| 4 | 197 |
| 2 | 198 |
| 1 | 198 |

```postgresql
with gte_2021_closed_orders as (select o.order_id
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     u_frequency as (select user_id, count(order_id) order_cnt
                     from orders o
                              join users u on o.user_id = u.id
                     where o.order_id in (select * from gte_2021_closed_orders)
                     group by user_id),

     tmp as (select user_id, order_cnt, ntile(5) over (order by order_cnt desc) frequency
                       from u_frequency
                       order by order_cnt desc, user_id)

select frequency, count(user_id)
from tmp
group by frequency
```

| frequency | count |
| :--- | :--- |
| 3 | 198 |
| 5 | 197 |
| 4 | 197 |
| 2 | 198 |
| 1 | 198 |

Как мы видим во всех случаях данные распределены почти поровну

### Тепер будем подробно разбирать по одному все кейсы
Recency мы получаем по этой локике

```postgresql
with gte_2021_closed_orders as (select o.order_id, user_id, payment, order_ts
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     u_recency as (select user_id, now() - max(order_ts) last_order_was
                   from gte_2021_closed_orders o
                            right join users u on o.user_id = u.id
                   group by user_id)
select user_id, last_order_was, ntile(5) over (order by last_order_was desc) recency
from u_recency
order by recency desc
limit 10;
```

| user\_id | last\_order\_was | recency |
| :--- | :--- | :--- |
| 810 | 0 years 0 mons 42 days 19 hours 23 mins 6.729532 secs | 5 |
| 734 | 0 years 0 mons 42 days 18 hours 40 mins 21.729532 secs | 5 |
| 16 | 0 years 0 mons 42 days 19 hours 45 mins 36.729532 secs | 5 |
| 421 | 0 years 0 mons 42 days 19 hours 31 mins 26.729532 secs | 5 |
| 600 | 0 years 0 mons 42 days 19 hours 13 mins 31.729532 secs | 5 |
| 330 | 0 years 0 mons 42 days 18 hours 44 mins 1.729532 secs | 5 |
| 22 | 0 years 0 mons 42 days 20 hours 3 mins 46.729532 secs | 5 |
| 680 | 0 years 0 mons 42 days 20 hours 1 mins 6.729532 secs | 5 |
| 201 | 0 years 0 mons 42 days 19 hours 36 mins 9.729532 secs | 5 |
| 299 | 0 years 0 mons 42 days 18 hours 19 mins 49.729532 secs | 5 |

Тепер frequency

```postgresql
with gte_2021_closed_orders as (select o.order_id, user_id, payment, order_ts
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     u_frequency as (select user_id, count(order_id) orders_cnt
                     from gte_2021_closed_orders o
                              join users u on o.user_id = u.id
                     group by user_id)
select user_id, orders_cnt, ntile(5) over (order by orders_cnt) frequency
from u_frequency
order by orders_cnt desc, user_id
limit 10;
```
| user\_id | orders\_cnt | frequency |
| :--- | :--- | :--- |
| 684 | 15 | 5 |
| 65 | 12 | 5 |
| 330 | 12 | 5 |
| 488 | 12 | 5 |
| 517 | 12 | 5 |
| 540 | 12 | 5 |
| 788 | 12 | 5 |
| 56 | 11 | 5 |
| 105 | 11 | 5 |
| 184 | 11 | 5 |

И monetary 

```postgresql
with gte_2021_closed_orders as (select o.order_id, user_id, payment, order_ts
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     u_monetary as (select user_id, sum(payment) spent
                    from gte_2021_closed_orders o
                             join users u on o.user_id = u.id
                    group by user_id)
select user_id, spent, ntile(5) over (order by spent) monetary
from u_monetary
order by spent desc;
```

| user\_id | spent | monetary |
| :--- | :--- | :--- |
| 684 | 37500 | 5 |
| 563 | 36840 | 5 |
| 940 | 31980 | 5 |
| 735 | 31320 | 5 |
| 725 | 29640 | 5 |
| 755 | 29580 | 5 |
| 387 | 29340 | 5 |
| 56 | 29040 | 5 |
| 788 | 28980 | 5 |
| 858 | 28800 | 5 |
| 517 | 28680 | 5 |
| 537 | 28440 | 5 |
| 585 | 27900 | 5 |
| 442 | 27780 | 5 |
| 751 | 27780 | 5 |
| 566 | 27540 | 5 |
| 510 | 27420 | 5 |
| 488 | 27060 | 5 |
| 60 | 26820 | 5 |
| 803 | 26520 | 5 |
| 729 | 26520 | 5 |
| 614 | 26460 | 5 |
| 414 | 26100 | 5 |
| 845 | 25620 | 5 |
| 184 | 25560 | 5 |
| 931 | 25500 | 5 |
| 828 | 25320 | 5 |
| 643 | 25200 | 5 |
| 540 | 24780 | 5 |
| 528 | 24780 | 5 |
| 504 | 24420 | 5 |
| 956 | 24240 | 5 |
| 574 | 24120 | 5 |
| 330 | 24000 | 5 |
| 742 | 24000 | 5 |
| 745 | 23940 | 5 |
| 536 | 23940 | 5 |
| 101 | 23760 | 5 |
| 375 | 23640 | 5 |
| 986 | 23520 | 5 |
| 547 | 23280 | 5 |
| 800 | 23160 | 5 |
| 863 | 23040 | 5 |
| 993 | 22980 | 5 |
| 519 | 22800 | 5 |
| 767 | 22740 | 5 |
| 268 | 22740 | 5 |
| 911 | 22680 | 5 |
| 912 | 22620 | 5 |
| 156 | 22620 | 5 |
| 491 | 22620 | 5 |
| 308 | 22560 | 5 |
| 564 | 22440 | 5 |
| 582 | 22320 | 5 |
| 55 | 22200 | 5 |
| 834 | 22200 | 5 |
| 384 | 22200 | 5 |
| 864 | 22140 | 5 |
| 297 | 22080 | 5 |
| 797 | 22080 | 5 |
| 406 | 21960 | 5 |
| 215 | 21720 | 5 |
| 957 | 21600 | 5 |
| 937 | 21600 | 5 |
| 532 | 21420 | 5 |
| 686 | 21360 | 5 |
| 263 | 21360 | 5 |
| 143 | 21000 | 5 |
| 499 | 21000 | 5 |
| 765 | 20940 | 5 |
| 262 | 20940 | 5 |
| 402 | 20880 | 5 |
| 997 | 20760 | 5 |
| 485 | 20760 | 5 |
| 94 | 20700 | 5 |
| 369 | 20640 | 5 |
| 687 | 20640 | 5 |
| 944 | 20580 | 5 |
| 265 | 20580 | 5 |
| 105 | 20520 | 5 |
| 599 | 20460 | 5 |
| 65 | 20400 | 5 |
| 394 | 20340 | 5 |
| 38 | 20340 | 5 |
| 465 | 20220 | 5 |
| 149 | 20100 | 5 |
| 259 | 20100 | 5 |
| 970 | 20100 | 5 |
| 119 | 20040 | 5 |
| 173 | 20040 | 5 |
| 264 | 19920 | 5 |
| 794 | 19920 | 5 |
| 769 | 19920 | 5 |
| 771 | 19920 | 5 |
| 368 | 19920 | 5 |
| 502 | 19860 | 5 |
| 979 | 19860 | 5 |
| 229 | 19800 | 5 |
| 5 | 19680 | 5 |
| 267 | 19680 | 5 |
| 703 | 19560 | 5 |
| 556 | 19560 | 5 |
| 196 | 19500 | 5 |
| 82 | 19440 | 5 |
| 426 | 19380 | 5 |
| 429 | 19380 | 5 |
| 350 | 19380 | 5 |
| 52 | 19260 | 5 |
| 278 | 19260 | 5 |
| 164 | 19260 | 5 |
| 11 | 19200 | 5 |
| 545 | 19200 | 5 |
| 741 | 19200 | 5 |
| 19 | 19140 | 5 |
| 829 | 19140 | 5 |
| 161 | 19080 | 5 |
| 206 | 19080 | 5 |
| 942 | 19020 | 5 |
| 457 | 18960 | 5 |
| 475 | 18900 | 5 |
| 449 | 18780 | 5 |
| 668 | 18780 | 5 |
| 917 | 18780 | 5 |
| 905 | 18780 | 5 |
| 81 | 18720 | 5 |
| 737 | 18660 | 5 |
| 676 | 18660 | 5 |
| 396 | 18660 | 5 |
| 709 | 18660 | 5 |
| 332 | 18540 | 5 |
| 631 | 18480 | 5 |
| 231 | 18480 | 5 |
| 270 | 18420 | 5 |
| 170 | 18420 | 5 |
| 347 | 18420 | 5 |
| 84 | 18420 | 5 |
| 100 | 18360 | 5 |
| 939 | 18360 | 5 |
| 568 | 18300 | 5 |
| 76 | 18240 | 5 |
| 994 | 18240 | 5 |
| 309 | 18180 | 5 |
| 554 | 18180 | 5 |
| 6 | 17940 | 5 |
| 322 | 17940 | 5 |
| 832 | 17880 | 5 |
| 108 | 17880 | 5 |
| 227 | 17820 | 5 |
| 280 | 17820 | 5 |
| 705 | 17820 | 5 |
| 289 | 17820 | 5 |
| 682 | 17760 | 5 |
| 35 | 17700 | 5 |
| 325 | 17700 | 5 |
| 901 | 17640 | 5 |
| 209 | 17580 | 5 |
| 2 | 17580 | 5 |
| 907 | 17580 | 5 |
| 221 | 17520 | 5 |
| 223 | 17520 | 5 |
| 991 | 17520 | 5 |
| 106 | 17460 | 5 |
| 48 | 17460 | 5 |
| 717 | 17400 | 5 |
| 169 | 17400 | 5 |
| 508 | 17340 | 5 |
| 15 | 17340 | 5 |
| 782 | 17220 | 5 |
| 372 | 17100 | 5 |
| 633 | 17040 | 5 |
| 526 | 17040 | 5 |
| 118 | 17040 | 5 |
| 107 | 17040 | 5 |
| 732 | 16980 | 5 |
| 861 | 16980 | 5 |
| 411 | 16920 | 5 |
| 219 | 16920 | 5 |
| 358 | 16920 | 5 |
| 561 | 16920 | 5 |
| 98 | 16860 | 5 |
| 187 | 16800 | 5 |
| 876 | 16800 | 5 |
| 598 | 16800 | 5 |
| 188 | 16740 | 5 |
| 606 | 16740 | 5 |
| 992 | 16680 | 5 |
| 44 | 16680 | 5 |
| 349 | 16620 | 5 |
| 404 | 16560 | 5 |
| 971 | 16560 | 5 |
| 814 | 16560 | 5 |
| 758 | 16560 | 5 |
| 210 | 16500 | 5 |
| 337 | 16440 | 5 |
| 571 | 16440 | 5 |
| 235 | 16380 | 5 |
| 792 | 16380 | 5 |
| 461 | 16320 | 4 |
| 938 | 16320 | 4 |
| 879 | 16320 | 4 |
| 515 | 16320 | 4 |
| 99 | 16260 | 4 |
| 47 | 16260 | 4 |
| 453 | 16200 | 4 |
| 630 | 16200 | 4 |
| 102 | 16200 | 4 |
| 663 | 16140 | 4 |
| 477 | 16140 | 4 |
| 493 | 16140 | 4 |
| 531 | 16020 | 4 |
| 323 | 16020 | 4 |
| 602 | 16020 | 4 |
| 86 | 15960 | 4 |
| 891 | 15960 | 4 |
| 711 | 15960 | 4 |
| 437 | 15900 | 4 |
| 768 | 15900 | 4 |
| 351 | 15840 | 4 |
| 557 | 15780 | 4 |
| 380 | 15720 | 4 |
| 207 | 15660 | 4 |
| 838 | 15660 | 4 |
| 756 | 15600 | 4 |
| 968 | 15540 | 4 |
| 431 | 15540 | 4 |
| 683 | 15540 | 4 |
| 490 | 15480 | 4 |
| 174 | 15420 | 4 |
| 96 | 15420 | 4 |
| 972 | 15360 | 4 |
| 468 | 15360 | 4 |
| 315 | 15360 | 4 |
| 588 | 15360 | 4 |
| 489 | 15360 | 4 |
| 892 | 15360 | 4 |
| 45 | 15360 | 4 |
| 868 | 15300 | 4 |
| 629 | 15300 | 4 |
| 18 | 15240 | 4 |
| 945 | 15180 | 4 |
| 914 | 15180 | 4 |
| 634 | 15120 | 4 |
| 790 | 15120 | 4 |
| 793 | 15120 | 4 |
| 569 | 15120 | 4 |
| 496 | 15060 | 4 |
| 653 | 15060 | 4 |
| 455 | 15060 | 4 |
| 271 | 15060 | 4 |
| 801 | 15060 | 4 |
| 833 | 15000 | 4 |
| 857 | 15000 | 4 |
| 550 | 14940 | 4 |
| 415 | 14940 | 4 |
| 269 | 14940 | 4 |
| 553 | 14940 | 4 |
| 565 | 14940 | 4 |
| 290 | 14880 | 4 |
| 670 | 14880 | 4 |
| 176 | 14880 | 4 |
| 880 | 14880 | 4 |
| 377 | 14820 | 4 |
| 33 | 14760 | 4 |
| 242 | 14760 | 4 |
| 935 | 14700 | 4 |
| 736 | 14580 | 4 |
| 320 | 14580 | 4 |
| 646 | 14520 | 4 |
| 589 | 14520 | 4 |
| 198 | 14520 | 4 |
| 529 | 14520 | 4 |
| 471 | 14520 | 4 |
| 439 | 14460 | 4 |
| 370 | 14400 | 4 |
| 346 | 14340 | 4 |
| 883 | 14340 | 4 |
| 822 | 14340 | 4 |
| 448 | 14340 | 4 |
| 204 | 14340 | 4 |
| 644 | 14280 | 4 |
| 476 | 14280 | 4 |
| 918 | 14220 | 4 |
| 954 | 14220 | 4 |
| 51 | 14220 | 4 |
| 982 | 14220 | 4 |
| 674 | 14160 | 4 |
| 228 | 14160 | 4 |
| 113 | 14100 | 4 |
| 615 | 14100 | 4 |
| 123 | 14100 | 4 |
| 58 | 14100 | 4 |
| 146 | 14040 | 4 |
| 874 | 13920 | 4 |
| 928 | 13920 | 4 |
| 486 | 13860 | 4 |
| 716 | 13860 | 4 |
| 329 | 13860 | 4 |
| 541 | 13860 | 4 |
| 257 | 13800 | 4 |
| 177 | 13800 | 4 |
| 397 | 13800 | 4 |
| 87 | 13800 | 4 |
| 842 | 13800 | 4 |
| 692 | 13740 | 4 |
| 305 | 13740 | 4 |
| 898 | 13740 | 4 |
| 962 | 13740 | 4 |
| 286 | 13680 | 4 |
| 915 | 13680 | 4 |
| 194 | 13680 | 4 |
| 446 | 13620 | 4 |
| 423 | 13560 | 4 |
| 232 | 13560 | 4 |
| 140 | 13560 | 4 |
| 761 | 13500 | 4 |
| 133 | 13500 | 4 |
| 651 | 13500 | 4 |
| 770 | 13500 | 4 |
| 530 | 13440 | 4 |
| 890 | 13380 | 4 |
| 462 | 13380 | 4 |
| 360 | 13380 | 4 |
| 197 | 13380 | 4 |
| 124 | 13320 | 4 |
| 764 | 13320 | 4 |
| 853 | 13320 | 4 |
| 534 | 13320 | 4 |
| 826 | 13320 | 4 |
| 552 | 13320 | 4 |
| 311 | 13320 | 4 |
| 714 | 13260 | 4 |
| 851 | 13260 | 4 |
| 193 | 13260 | 4 |
| 216 | 13200 | 4 |
| 699 | 13200 | 4 |
| 392 | 13200 | 4 |
| 501 | 13200 | 4 |
| 570 | 13140 | 4 |
| 723 | 13140 | 4 |
| 241 | 13140 | 4 |
| 613 | 13140 | 4 |
| 200 | 13140 | 4 |
| 218 | 13140 | 4 |
| 638 | 13080 | 4 |
| 75 | 13080 | 4 |
| 78 | 13080 | 4 |
| 878 | 13020 | 4 |
| 66 | 13020 | 4 |
| 180 | 12960 | 4 |
| 712 | 12960 | 4 |
| 391 | 12960 | 4 |
| 240 | 12960 | 4 |
| 523 | 12900 | 4 |
| 507 | 12900 | 4 |
| 135 | 12900 | 4 |
| 16 | 12900 | 4 |
| 678 | 12840 | 4 |
| 580 | 12840 | 4 |
| 158 | 12840 | 4 |
| 624 | 12840 | 4 |
| 136 | 12840 | 4 |
| 604 | 12840 | 4 |
| 584 | 12780 | 4 |
| 825 | 12780 | 4 |
| 401 | 12780 | 4 |
| 873 | 12780 | 4 |
| 780 | 12780 | 4 |
| 151 | 12720 | 4 |
| 39 | 12720 | 4 |
| 165 | 12720 | 4 |
| 809 | 12720 | 4 |
| 373 | 12720 | 4 |
| 336 | 12720 | 4 |
| 192 | 12660 | 4 |
| 607 | 12660 | 4 |
| 382 | 12660 | 4 |
| 425 | 12660 | 4 |
| 0 | 12600 | 4 |
| 946 | 12600 | 4 |
| 342 | 12600 | 4 |
| 361 | 12600 | 4 |
| 511 | 12540 | 4 |
| 155 | 12540 | 4 |
| 459 | 12540 | 4 |
| 41 | 12540 | 4 |
| 131 | 12480 | 4 |
| 877 | 12480 | 4 |
| 412 | 12480 | 4 |
| 921 | 12420 | 4 |
| 909 | 12420 | 4 |
| 407 | 12420 | 4 |
| 619 | 12360 | 4 |
| 722 | 12360 | 4 |
| 17 | 12360 | 4 |
| 577 | 12300 | 3 |
| 718 | 12300 | 3 |
| 899 | 12240 | 3 |
| 199 | 12240 | 3 |
| 740 | 12240 | 3 |
| 27 | 12240 | 3 |
| 331 | 12240 | 3 |
| 953 | 12180 | 3 |
| 862 | 12180 | 3 |
| 29 | 12180 | 3 |
| 576 | 12120 | 3 |
| 747 | 12120 | 3 |
| 385 | 12120 | 3 |
| 492 | 12120 | 3 |
| 138 | 12060 | 3 |
| 409 | 12060 | 3 |
| 20 | 12000 | 3 |
| 72 | 12000 | 3 |
| 195 | 12000 | 3 |
| 779 | 12000 | 3 |
| 983 | 11940 | 3 |
| 115 | 11940 | 3 |
| 205 | 11940 | 3 |
| 355 | 11880 | 3 |
| 609 | 11880 | 3 |
| 307 | 11880 | 3 |
| 160 | 11820 | 3 |
| 738 | 11820 | 3 |
| 559 | 11760 | 3 |
| 273 | 11760 | 3 |
| 866 | 11760 | 3 |
| 947 | 11760 | 3 |
| 480 | 11760 | 3 |
| 987 | 11760 | 3 |
| 827 | 11700 | 3 |
| 249 | 11700 | 3 |
| 603 | 11700 | 3 |
| 1 | 11700 | 3 |
| 963 | 11640 | 3 |
| 443 | 11640 | 3 |
| 23 | 11640 | 3 |
| 378 | 11640 | 3 |
| 620 | 11580 | 3 |
| 990 | 11580 | 3 |
| 244 | 11580 | 3 |
| 841 | 11580 | 3 |
| 95 | 11580 | 3 |
| 321 | 11580 | 3 |
| 150 | 11580 | 3 |
| 766 | 11580 | 3 |
| 816 | 11520 | 3 |
| 282 | 11520 | 3 |
| 988 | 11520 | 3 |
| 456 | 11520 | 3 |
| 112 | 11520 | 3 |
| 672 | 11460 | 3 |
| 53 | 11460 | 3 |
| 739 | 11400 | 3 |
| 3 | 11400 | 3 |
| 702 | 11400 | 3 |
| 513 | 11400 | 3 |
| 433 | 11340 | 3 |
| 835 | 11340 | 3 |
| 744 | 11340 | 3 |
| 139 | 11340 | 3 |
| 292 | 11340 | 3 |
| 234 | 11340 | 3 |
| 474 | 11280 | 3 |
| 182 | 11280 | 3 |
| 632 | 11280 | 3 |
| 843 | 11280 | 3 |
| 897 | 11280 | 3 |
| 279 | 11220 | 3 |
| 312 | 11220 | 3 |
| 299 | 11220 | 3 |
| 820 | 11220 | 3 |
| 251 | 11220 | 3 |
| 525 | 11160 | 3 |
| 967 | 11160 | 3 |
| 733 | 11160 | 3 |
| 441 | 11160 | 3 |
| 539 | 11040 | 3 |
| 660 | 11040 | 3 |
| 846 | 11040 | 3 |
| 67 | 11040 | 3 |
| 134 | 10980 | 3 |
| 326 | 10980 | 3 |
| 201 | 10980 | 3 |
| 611 | 10920 | 3 |
| 929 | 10920 | 3 |
| 774 | 10860 | 3 |
| 731 | 10860 | 3 |
| 865 | 10860 | 3 |
| 386 | 10800 | 3 |
| 887 | 10800 | 3 |
| 772 | 10800 | 3 |
| 836 | 10800 | 3 |
| 661 | 10800 | 3 |
| 37 | 10800 | 3 |
| 277 | 10800 | 3 |
| 626 | 10740 | 3 |
| 246 | 10740 | 3 |
| 804 | 10740 | 3 |
| 178 | 10740 | 3 |
| 487 | 10740 | 3 |
| 810 | 10740 | 3 |
| 941 | 10680 | 3 |
| 961 | 10680 | 3 |
| 64 | 10620 | 3 |
| 272 | 10620 | 3 |
| 870 | 10620 | 3 |
| 92 | 10560 | 3 |
| 701 | 10560 | 3 |
| 520 | 10560 | 3 |
| 969 | 10560 | 3 |
| 932 | 10560 | 3 |
| 659 | 10560 | 3 |
| 154 | 10560 | 3 |
| 594 | 10500 | 3 |
| 754 | 10500 | 3 |
| 593 | 10440 | 3 |
| 129 | 10440 | 3 |
| 647 | 10440 | 3 |
| 137 | 10440 | 3 |
| 727 | 10380 | 3 |
| 460 | 10380 | 3 |
| 69 | 10380 | 3 |
| 500 | 10380 | 3 |
| 316 | 10320 | 3 |
| 936 | 10320 | 3 |
| 575 | 10320 | 3 |
| 213 | 10320 | 3 |
| 549 | 10260 | 3 |
| 162 | 10260 | 3 |
| 815 | 10260 | 3 |
| 844 | 10200 | 3 |
| 49 | 10200 | 3 |
| 645 | 10200 | 3 |
| 664 | 10140 | 3 |
| 294 | 10140 | 3 |
| 400 | 10140 | 3 |
| 555 | 10080 | 3 |
| 665 | 10080 | 3 |
| 393 | 10080 | 3 |
| 950 | 10080 | 3 |
| 185 | 10080 | 3 |
| 999 | 10020 | 3 |
| 85 | 10020 | 3 |
| 680 | 10020 | 3 |
| 818 | 10020 | 3 |
| 786 | 10020 | 3 |
| 807 | 10020 | 3 |
| 310 | 10020 | 3 |
| 147 | 10020 | 3 |
| 54 | 9960 | 3 |
| 363 | 9960 | 3 |
| 8 | 9960 | 3 |
| 222 | 9960 | 3 |
| 343 | 9960 | 3 |
| 978 | 9960 | 3 |
| 548 | 9900 | 3 |
| 4 | 9900 | 3 |
| 464 | 9900 | 3 |
| 417 | 9900 | 3 |
| 168 | 9900 | 3 |
| 595 | 9840 | 3 |
| 481 | 9840 | 3 |
| 920 | 9840 | 3 |
| 773 | 9840 | 3 |
| 640 | 9840 | 3 |
| 830 | 9840 | 3 |
| 590 | 9840 | 3 |
| 32 | 9780 | 3 |
| 79 | 9780 | 3 |
| 776 | 9780 | 3 |
| 789 | 9780 | 3 |
| 943 | 9720 | 3 |
| 233 | 9720 | 3 |
| 753 | 9720 | 3 |
| 416 | 9720 | 3 |
| 885 | 9660 | 3 |
| 706 | 9660 | 3 |
| 381 | 9600 | 3 |
| 163 | 9600 | 3 |
| 655 | 9600 | 3 |
| 494 | 9540 | 3 |
| 648 | 9540 | 3 |
| 625 | 9540 | 3 |
| 617 | 9480 | 3 |
| 258 | 9480 | 3 |
| 484 | 9480 | 3 |
| 966 | 9480 | 3 |
| 327 | 9480 | 3 |
| 250 | 9480 | 3 |
| 608 | 9420 | 2 |
| 367 | 9420 | 3 |
| 354 | 9420 | 3 |
| 324 | 9420 | 3 |
| 57 | 9420 | 3 |
| 984 | 9360 | 2 |
| 344 | 9360 | 2 |
| 238 | 9360 | 2 |
| 357 | 9300 | 2 |
| 104 | 9300 | 2 |
| 691 | 9300 | 2 |
| 447 | 9300 | 2 |
| 925 | 9300 | 2 |
| 183 | 9300 | 2 |
| 275 | 9240 | 2 |
| 432 | 9240 | 2 |
| 296 | 9240 | 2 |
| 40 | 9240 | 2 |
| 389 | 9240 | 2 |
| 558 | 9240 | 2 |
| 817 | 9240 | 2 |
| 612 | 9240 | 2 |
| 799 | 9240 | 2 |
| 436 | 9240 | 2 |
| 662 | 9180 | 2 |
| 708 | 9180 | 2 |
| 285 | 9180 | 2 |
| 438 | 9180 | 2 |
| 656 | 9180 | 2 |
| 144 | 9120 | 2 |
| 916 | 9120 | 2 |
| 641 | 9060 | 2 |
| 335 | 9060 | 2 |
| 9 | 9060 | 2 |
| 430 | 9060 | 2 |
| 996 | 9060 | 2 |
| 440 | 9060 | 2 |
| 31 | 9000 | 2 |
| 791 | 9000 | 2 |
| 179 | 8940 | 2 |
| 509 | 8940 | 2 |
| 783 | 8940 | 2 |
| 720 | 8940 | 2 |
| 906 | 8880 | 2 |
| 256 | 8880 | 2 |
| 247 | 8880 | 2 |
| 167 | 8880 | 2 |
| 798 | 8880 | 2 |
| 364 | 8820 | 2 |
| 120 | 8820 | 2 |
| 293 | 8820 | 2 |
| 700 | 8820 | 2 |
| 812 | 8820 | 2 |
| 230 | 8760 | 2 |
| 535 | 8760 | 2 |
| 976 | 8760 | 2 |
| 175 | 8760 | 2 |
| 253 | 8760 | 2 |
| 91 | 8700 | 2 |
| 610 | 8700 | 2 |
| 340 | 8700 | 2 |
| 974 | 8700 | 2 |
| 848 | 8700 | 2 |
| 472 | 8700 | 2 |
| 775 | 8700 | 2 |
| 998 | 8640 | 2 |
| 781 | 8640 | 2 |
| 605 | 8580 | 2 |
| 212 | 8580 | 2 |
| 422 | 8520 | 2 |
| 922 | 8520 | 2 |
| 667 | 8520 | 2 |
| 587 | 8520 | 2 |
| 895 | 8520 | 2 |
| 70 | 8520 | 2 |
| 353 | 8520 | 2 |
| 635 | 8520 | 2 |
| 117 | 8520 | 2 |
| 157 | 8520 | 2 |
| 89 | 8460 | 2 |
| 116 | 8460 | 2 |
| 679 | 8460 | 2 |
| 181 | 8460 | 2 |
| 512 | 8460 | 2 |
| 854 | 8460 | 2 |
| 675 | 8460 | 2 |
| 254 | 8460 | 2 |
| 886 | 8400 | 2 |
| 516 | 8400 | 2 |
| 190 | 8400 | 2 |
| 497 | 8400 | 2 |
| 690 | 8400 | 2 |
| 30 | 8400 | 2 |
| 435 | 8400 | 2 |
| 328 | 8340 | 2 |
| 341 | 8340 | 2 |
| 61 | 8280 | 2 |
| 10 | 8280 | 2 |
| 654 | 8220 | 2 |
| 934 | 8220 | 2 |
| 721 | 8220 | 2 |
| 390 | 8220 | 2 |
| 252 | 8220 | 2 |
| 398 | 8160 | 2 |
| 359 | 8160 | 2 |
| 955 | 8160 | 2 |
| 46 | 8160 | 2 |
| 420 | 8100 | 2 |
| 42 | 8100 | 2 |
| 750 | 8100 | 2 |
| 239 | 8100 | 2 |
| 658 | 8100 | 2 |
| 698 | 8040 | 2 |
| 479 | 8040 | 2 |
| 28 | 8040 | 2 |
| 128 | 7980 | 2 |
| 7 | 7980 | 2 |
| 707 | 7980 | 2 |
| 424 | 7980 | 2 |
| 596 | 7980 | 2 |
| 418 | 7980 | 2 |
| 869 | 7980 | 2 |
| 470 | 7980 | 2 |
| 63 | 7920 | 2 |
| 145 | 7860 | 2 |
| 25 | 7860 | 2 |
| 777 | 7860 | 2 |
| 403 | 7860 | 2 |
| 26 | 7800 | 2 |
| 434 | 7800 | 2 |
| 217 | 7800 | 2 |
| 366 | 7800 | 2 |
| 450 | 7800 | 2 |
| 924 | 7740 | 2 |
| 882 | 7740 | 2 |
| 697 | 7740 | 2 |
| 498 | 7740 | 2 |
| 811 | 7680 | 2 |
| 283 | 7680 | 2 |
| 600 | 7680 | 2 |
| 787 | 7680 | 2 |
| 913 | 7680 | 2 |
| 958 | 7680 | 2 |
| 220 | 7620 | 2 |
| 362 | 7620 | 2 |
| 36 | 7620 | 2 |
| 533 | 7560 | 2 |
| 410 | 7560 | 2 |
| 933 | 7560 | 2 |
| 73 | 7500 | 2 |
| 693 | 7500 | 2 |
| 142 | 7500 | 2 |
| 226 | 7500 | 2 |
| 696 | 7440 | 2 |
| 298 | 7380 | 2 |
| 166 | 7380 | 2 |
| 572 | 7380 | 2 |
| 68 | 7380 | 2 |
| 538 | 7320 | 2 |
| 639 | 7320 | 2 |
| 669 | 7260 | 2 |
| 960 | 7200 | 2 |
| 214 | 7200 | 2 |
| 300 | 7200 | 2 |
| 719 | 7200 | 2 |
| 171 | 7140 | 2 |
| 313 | 7080 | 2 |
| 463 | 7080 | 2 |
| 62 | 7080 | 2 |
| 852 | 7020 | 2 |
| 421 | 7020 | 2 |
| 592 | 6960 | 2 |
| 483 | 6960 | 2 |
| 132 | 6960 | 2 |
| 408 | 6960 | 2 |
| 452 | 6960 | 2 |
| 236 | 6900 | 2 |
| 225 | 6900 | 2 |
| 778 | 6900 | 2 |
| 623 | 6900 | 2 |
| 125 | 6900 | 2 |
| 908 | 6840 | 2 |
| 964 | 6840 | 2 |
| 715 | 6840 | 2 |
| 458 | 6840 | 2 |
| 685 | 6840 | 2 |
| 121 | 6780 | 2 |
| 597 | 6780 | 2 |
| 419 | 6780 | 2 |
| 746 | 6720 | 2 |
| 650 | 6720 | 2 |
| 71 | 6720 | 2 |
| 980 | 6720 | 2 |
| 995 | 6720 | 2 |
| 306 | 6660 | 2 |
| 83 | 6660 | 2 |
| 796 | 6660 | 2 |
| 127 | 6660 | 2 |
| 301 | 6540 | 2 |
| 413 | 6540 | 2 |
| 395 | 6540 | 2 |
| 542 | 6540 | 2 |
| 405 | 6540 | 1 |
| 454 | 6480 | 1 |
| 562 | 6480 | 1 |
| 12 | 6480 | 1 |
| 581 | 6480 | 1 |
| 345 | 6420 | 1 |
| 111 | 6420 | 1 |
| 622 | 6420 | 1 |
| 383 | 6360 | 1 |
| 291 | 6360 | 1 |
| 482 | 6360 | 1 |
| 287 | 6360 | 1 |
| 260 | 6360 | 1 |
| 284 | 6300 | 1 |
| 903 | 6240 | 1 |
| 159 | 6180 | 1 |
| 652 | 6120 | 1 |
| 973 | 6120 | 1 |
| 573 | 6120 | 1 |
| 618 | 6120 | 1 |
| 24 | 6120 | 1 |
| 93 | 6060 | 1 |
| 673 | 6060 | 1 |
| 317 | 6060 | 1 |
| 13 | 6000 | 1 |
| 649 | 6000 | 1 |
| 628 | 5880 | 1 |
| 802 | 5820 | 1 |
| 637 | 5820 | 1 |
| 14 | 5820 | 1 |
| 724 | 5820 | 1 |
| 524 | 5820 | 1 |
| 904 | 5760 | 1 |
| 374 | 5760 | 1 |
| 126 | 5760 | 1 |
| 759 | 5700 | 1 |
| 191 | 5700 | 1 |
| 579 | 5640 | 1 |
| 749 | 5640 | 1 |
| 445 | 5640 | 1 |
| 578 | 5580 | 1 |
| 806 | 5580 | 1 |
| 855 | 5580 | 1 |
| 428 | 5520 | 1 |
| 627 | 5520 | 1 |
| 34 | 5520 | 1 |
| 867 | 5460 | 1 |
| 473 | 5460 | 1 |
| 583 | 5460 | 1 |
| 757 | 5400 | 1 |
| 927 | 5400 | 1 |
| 90 | 5400 | 1 |
| 872 | 5340 | 1 |
| 694 | 5340 | 1 |
| 551 | 5340 | 1 |
| 671 | 5280 | 1 |
| 318 | 5280 | 1 |
| 506 | 5280 | 1 |
| 153 | 5280 | 1 |
| 376 | 5280 | 1 |
| 352 | 5220 | 1 |
| 261 | 5160 | 1 |
| 728 | 5160 | 1 |
| 333 | 5100 | 1 |
| 734 | 5100 | 1 |
| 850 | 5040 | 1 |
| 642 | 5040 | 1 |
| 713 | 4980 | 1 |
| 141 | 4980 | 1 |
| 546 | 4980 | 1 |
| 304 | 4980 | 1 |
| 503 | 4920 | 1 |
| 985 | 4920 | 1 |
| 902 | 4860 | 1 |
| 621 | 4860 | 1 |
| 881 | 4800 | 1 |
| 601 | 4800 | 1 |
| 875 | 4740 | 1 |
| 208 | 4740 | 1 |
| 22 | 4740 | 1 |
| 975 | 4740 | 1 |
| 677 | 4680 | 1 |
| 444 | 4680 | 1 |
| 122 | 4680 | 1 |
| 923 | 4680 | 1 |
| 896 | 4620 | 1 |
| 743 | 4620 | 1 |
| 527 | 4620 | 1 |
| 302 | 4620 | 1 |
| 752 | 4560 | 1 |
| 948 | 4560 | 1 |
| 544 | 4560 | 1 |
| 847 | 4560 | 1 |
| 871 | 4500 | 1 |
| 762 | 4500 | 1 |
| 926 | 4440 | 1 |
| 319 | 4440 | 1 |
| 989 | 4440 | 1 |
| 74 | 4440 | 1 |
| 808 | 4380 | 1 |
| 43 | 4320 | 1 |
| 636 | 4320 | 1 |
| 521 | 4320 | 1 |
| 785 | 4320 | 1 |
| 203 | 4320 | 1 |
| 951 | 4260 | 1 |
| 334 | 4260 | 1 |
| 245 | 4260 | 1 |
| 77 | 4200 | 1 |
| 567 | 4200 | 1 |
| 900 | 4200 | 1 |
| 281 | 4140 | 1 |
| 591 | 4140 | 1 |
| 952 | 4080 | 1 |
| 314 | 4080 | 1 |
| 688 | 4080 | 1 |
| 748 | 4080 | 1 |
| 399 | 4080 | 1 |
| 348 | 4080 | 1 |
| 237 | 4080 | 1 |
| 819 | 4020 | 1 |
| 186 | 4020 | 1 |
| 189 | 4020 | 1 |
| 888 | 3960 | 1 |
| 981 | 3960 | 1 |
| 681 | 3960 | 1 |
| 695 | 3900 | 1 |
| 616 | 3900 | 1 |
| 795 | 3900 | 1 |
| 560 | 3840 | 1 |
| 365 | 3840 | 1 |
| 586 | 3780 | 1 |
| 965 | 3720 | 1 |
| 919 | 3720 | 1 |
| 949 | 3720 | 1 |
| 823 | 3720 | 1 |
| 884 | 3720 | 1 |
| 726 | 3720 | 1 |
| 80 | 3660 | 1 |
| 856 | 3600 | 1 |
| 469 | 3600 | 1 |
| 88 | 3540 | 1 |
| 451 | 3540 | 1 |
| 495 | 3540 | 1 |
| 505 | 3480 | 1 |
| 288 | 3420 | 1 |
| 21 | 3420 | 1 |
| 295 | 3420 | 1 |
| 466 | 3360 | 1 |
| 59 | 3360 | 1 |
| 50 | 3300 | 1 |
| 356 | 3240 | 1 |
| 371 | 3240 | 1 |
| 657 | 3240 | 1 |
| 148 | 3180 | 1 |
| 666 | 3180 | 1 |
| 893 | 3000 | 1 |
| 339 | 3000 | 1 |
| 97 | 2940 | 1 |
| 894 | 2880 | 1 |
| 202 | 2820 | 1 |
| 522 | 2760 | 1 |
| 172 | 2700 | 1 |
| 959 | 2700 | 1 |
| 303 | 2640 | 1 |
| 763 | 2640 | 1 |
| 427 | 2580 | 1 |
| 813 | 2520 | 1 |
| 860 | 2520 | 1 |
| 543 | 2460 | 1 |
| 255 | 2400 | 1 |
| 839 | 2400 | 1 |
| 840 | 2400 | 1 |
| 710 | 2400 | 1 |
| 518 | 2400 | 1 |
| 859 | 2340 | 1 |
| 689 | 2340 | 1 |
| 760 | 2340 | 1 |
| 103 | 2280 | 1 |
| 849 | 2280 | 1 |
| 805 | 2280 | 1 |
| 152 | 2220 | 1 |
| 338 | 2220 | 1 |
| 109 | 2160 | 1 |
| 478 | 2160 | 1 |
| 114 | 2100 | 1 |
| 274 | 1800 | 1 |
| 910 | 1560 | 1 |
| 824 | 1500 | 1 |
| 248 | 1260 | 1 |
| 379 | 1260 | 1 |
| 704 | 960 | 1 |
| 130 | 900 | 1 |
| 266 | 780 | 1 |
| 831 | 660 | 1 |
| 110 | 480 | 1 |
| 388 | 480 | 1 |
| 243 | 360 | 1 |

<hr>

И слияем все в один запрос

```postgresql
with gte_2021_closed_orders as (select o.order_id, user_id, payment, order_ts
                                from orderstatuslog ol
                                         join orders o on ol.order_id = o.order_id
                                where status_id = 4
                                  and extract(year from order_ts) >= 2021),
     u_recency as (select user_id, now() - max(order_ts) last_order_was
                   from gte_2021_closed_orders o
                            right join users u on o.user_id = u.id
                   group by user_id),
     u_frequency as (select user_id, count(order_id) orders_cnt
                     from gte_2021_closed_orders o
                              join users u on o.user_id = u.id
                     group by user_id),
     u_monetary as (select user_id, sum(payment) spent
                    from gte_2021_closed_orders o
                             join users u on o.user_id = u.id
                    group by user_id)
select u_frequency.user_id,
--        last_order_was,
       ntile(5) over (order by last_order_was desc) as recency,
--        orders_cnt,
       ntile(5) over (order by orders_cnt)          as frequency,
--        spent,
       ntile(5) over (order by spent)               as monetary_value
from u_recency
         join u_frequency on u_recency.user_id = u_frequency.user_id
         join u_monetary on u_recency.user_id = u_monetary.user_id
order by spent desc;
```

| user\_id | recency | frequency | monetary\_value |
| :--- | :--- | :--- | :--- |
| 684 | 4 | 5 | 5 |
| 563 | 1 | 5 | 5 |
| 940 | 3 | 5 | 5 |
| 735 | 5 | 5 | 5 |
| 725 | 5 | 5 | 5 |
| 755 | 5 | 5 | 5 |
| 387 | 3 | 5 | 5 |
| 56 | 3 | 5 | 5 |
| 788 | 5 | 5 | 5 |
| 858 | 5 | 5 | 5 |




