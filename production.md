```mermaid
classDiagram
direction BT
class orderitems {
   integer product_id
   integer order_id
   varchar(2048) name
   numeric(19,5) price
   numeric(19,5) discount
   integer quantity
   integer id
}
class orders {
   timestamp order_ts
   integer user_id
   numeric(19,5) bonus_payment
   numeric(19,5) payment
   numeric(19,5) cost
   numeric(19,5) bonus_grant
   integer status
   integer order_id
}
class orderstatuses {
   varchar(255) key
   integer id
}
class orderstatuslog {
   integer order_id
   integer status_id
   timestamp dttm
   integer id
}
class products {
   varchar(2048) name
   numeric(19,5) price
   integer id
}
class users {
   varchar(2048) name
   varchar(2048) login
   integer id
}

orders  -->  orderitems : order_id
products  -->  orderitems : product_id:id
users  -->  orders : user_id:id
orders  -->  orderstatuslog : order_id
orderstatuses  -->  orderstatuslog : status_id:id
```