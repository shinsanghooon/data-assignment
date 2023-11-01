create table if not exists "torder".daily_stat as (
    select store_code 
        , business_day 
        , count(*) as order_count
        , sum(case when is_error_exists is true then 1 else 0 end) as error_count
    from (select *
            ,(to_timestamp(time_order, 'yyyy-mm-dd hh24:mi') - interval '7 hour')::Date as business_day
            from "torder".orders) as order_with_busines_day
    group by store_code, business_day
    order by store_code, business_day
);