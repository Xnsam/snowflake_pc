use warehouse compute_wh;

use database test_database;

drop database test_database;
drop database test_ingestion;



select *
from tasty_bytes.raw_pos.truck_dev;

set saved_query_id = LAST_QUERY_ID();
set saved_timestamp = CURRENT_TIMESTAMP;
update tasty_bytes.raw_pos.truck_dev t
set t.year = (YEAR(current_date()) - 1000);

-- > Time travel hands on assignment
show variables;

select year
from tasty_bytes.raw_pos.truck_dev
where truck_id = 1; -- 1026

select year
from tasty_bytes.raw_pos.truck_dev
AT(timestamp => $saved_timestamp)
where truck_id = 1; -- 2009

select year
from tasty_bytes.raw_pos.truck_dev
BEFORE(statement => $saved_query_id)
where truck_id = 2; -- 2015


-- > Permanent, transient and temporary tables
drop table tasty_bytes.raw_pos.truck_dev;

create transient table tasty_bytes.raw_pos.truck_transient
clone tasty_bytes.raw_pos.truck;

create temporary table tasty_bytes.raw_pos.truck_temporary
clone tasty_bytes.raw_pos.truck;

show tables like 'truck%';

-- > data retention to 90 days
alter table tasty_bytes.raw_pos.truck
set data_retention_time_in_days = 90;

alter table tasty_bytes.raw_pos.truck_transient
set data_retention_time_in_days = 90;

alter table  tasty_bytes.raw_pos.truck_temporary
set data_retention_time_in_days = 90;

show tables like 'truck%';

alter table tasty_bytes.raw_pos.truck_transient
set data_retention_time_in_days = 0;

alter table  tasty_bytes.raw_pos.truck_temporary
set data_retention_time_in_days = 0;

--- > Cloning hands on

create or replace table tasty_bytes.raw_pos.truck_clone
clone tasty_bytes.raw_pos.truck;

use database tasty_bytes;
use schema raw_pos;
show tables;

drop table tasty_bytes.raw_pos.truck_clone;

select *
from tasty_bytes.information_schema.table_storage_metrics
where (table_name = 'TRUCK_CLONE' or table_name = 'TRUCK')
and table_catalog = 'TASTY_BYTES';

insert into tasty_bytes.raw_pos.truck_clone
select * from tasty_bytes.raw_pos.truck;


create or replace schema tasty_bytes.raw_pos_clone
clone tasty_bytes.raw_pos;

create or replace database tasty_bytes_clone
clone tasty_bytes;

--> clone based on offset
create or replace table tasty_bytes.raw_pos.truck_clone_time_travel
clone tasty_bytes.raw_pos.truck at(offset => -60*0.1);

select * from tasty_bytes.raw_pos.truck_clone_time_travel;

create table tasty_bytes.raw_pos.truck_dev
    clone tasty_bytes.raw_pos.truck;



-- > Resorce monitors

create resource monitor tasty_test_rm
with
    credit_quota = 15 -- 20 credits
    frequency = daily -- reset the monitor daily
    start_timestamp = immediately -- begin tracking immediately
    triggers
        on 90 percent do notify; -- notify account admins at 80%
        -- on 100 percent do suspend -- suspend warehouse at 100 percent, let queries finish
        -- on 110 percent do suspend_immediate;

show resource monitors;

alter warehouse tasty_de_wh 
set resource_monitor = tasty_test_rm;

show resource monitors;

alter resource monitor tasty_test_rm
set credit_quota = 15;

show resource monitors;

drop resource monitor tasty_test_rm;

show resource monitors;

drop warehouse tasty_de_wh;

create warehouse tasty_de_wh;
show warehouses;

alter warehouse tasty_de_wh
set resource_monitor = tasty_test_rm;

use warehouse tasty_de_wh;
show resource monitors;


---> User defined functions 
use warehouse compute_wh;
alter warehouse compute_wh
set warehouse_size = 'xsmall';

show warehouses;

show databases;

use database tasty_bytes;

select abs(-14);

select upper('upper');

show functions like 'LA%';

select max(sale_price_usd)
from tasty_bytes.raw_pos.menu;

select count(*) 
from tasty_bytes.raw_pos.menu;

--> create a function
create function max_menu_price()
    returns number(5, 2)
    as 
    $$
        select max(sale_price_usd) from tasty_bytes.raw_pos.menu
    $$
    ;

show functions like 'max%';

select max_menu_price();

--> create function
create function max_menu_price_converted(usd_to_new number)
    returns number(5, 2)
    as
    $$
        select usd_to_new * max(sale_price_usd) from tasty_bytes.raw_pos.menu
    $$
    ;

select max_menu_price_converted(1.35);

--> create a python function
create function winsorize(val numeric, up_bound numeric, low_bound numeric)
returns numeric
language python
runtime_version = '3.11'
handler = 'winsorize_py'
as
$$
def winsorize_py(val, up_bound, low_bound):
    if val > up_bound:
        return up_bound
    elif val < low_bound:
        return low_bound
    else:
        return val
$$;

select winsorize(sale_price_usd, 11.0, 4.0) as binned_price, 
sale_price_usd
from tasty_bytes.raw_pos.menu limit 10;

--> UDTF 
create function menu_prices_above(price_floor number)
    returns table (item varchar, price number)
    as 
    $$
        select menu_item_name, sale_price_usd
        from tasty_bytes.raw_pos.menu
        where sale_price_usd > price_floor
        order by 2 desc
    $$
    ;

show functions like 'menu_prices%';

select * from table(menu_prices_above(15));

select * from table(menu_prices_above(15))
where item ilike '%chicken%';


show functions like '%current_timestamp%';

create function min_menu_price()
    returns number(5, 2)
    as
    $$
        select min(sale_price_usd) from tasty_bytes.raw_pos.menu
    $$
    ;

select min_menu_price();

show functions like 'min_menu_price%';

create function menu_prices_below(price_ceiling number)
    returns table(item varchar, price number)
    as
    $$
        select menu_item_name, sale_price_usd
            from tasty_bytes.raw_pos.menu
            where sale_price_usd < price_ceiling
            order by 2 desc
    $$
    ;

select *
from table(menu_prices_below(3));


-- > stored procedures
SHOW PROCEDURES;

select * 
from tasty_bytes_clone.raw_pos.order_header
limit 100;

select count(*)
from tasty_bytes_clone.raw_pos.order_header
limit 100;

select max(order_ts), min(order_ts)
from tasty_bytes_clone.raw_pos.order_header;

set max_ts = (select max(order_ts) from tasty_bytes_clone.raw_pos.order_header);

select $max_ts;

select $max_ts, dateadd('day', -180, $max_ts);

set cutoff_ts = (select dateadd('DAY', - 180, $max_ts));

select max(order_ts) from tasty_bytes_clone.raw_pos.order_header
where order_ts < $cutoff_ts;

use database tasty_bytes;

create or replace procedure delete_old()
    returns boolean
    language SQL
    as
    $$
    declare
        max_ts timestamp;
        cutoff_ts timestamp;
    begin
        max_ts := (select max(order_ts) from tasty_bytes_clone.raw_pos.order_header);
        cutoff_ts := (select dateadd('day', -180, :max_ts));
        delete from tasty_bytes_clone.raw_pos.order_header
        where order_ts < :cutoff_ts;
    end;
    $$
    ;

drop procedure delete_old();

show procedures like 'delete_old%';

describe procedure delete_old();

call delete_old();

select min(order_ts)
from tasty_bytes_clone.raw_pos.order_header;

select $cutoff_ts;


use database tasty_bytes_clone;

create or replace procedure increase_prices()
    returns boolean
    language sql
    as 
    $$ 
    update tasty_bytes_clone.raw_pos.menu
        set sale_price_usd = menu.sale_price_usd + 1;
    $$;

call increase_prices();

describe procedure increase_prices();

create or replace procedure decrease_mango_sticky_rice_price()
    returns boolean
    language sql
    as 
    $$
    update tasty_bytes_clone.raw_pos.menu
        set sale_price_usd = menu.sale_price_usd - 1
        where menu.menu_item_name = 'Mango Sticky Rice';
    $$;

call decrease_mango_sticky_rice_price();

show procedures like 'decrease_mango_sticky_rice_price%';

-- > RBAC

use role accountadmin;

create role tasty_de;

show grants to role tasty_de;

show grants to role accountadmin;

grant role tasty_de to user [username];


create role tasty_role;

show grants to role tasty_role;

grant create database on account to role tasty_role;

set current_user_name = (select current_user);

select $current_user_name;


grant role tasty_role to user AKSONSAM098;

use role tasty_role;

create warehouse taasty_test_wh2;

use role accountadmin;

show grants to user AKSONSAM098;

show grants to role useradmin;