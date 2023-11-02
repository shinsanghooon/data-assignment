create table if not exists "torder".monthly_stat_by_store as (
    select store_code
        ,to_date(to_char(business_day, 'yyyy-mm-dd'), 'yyyy-mm') as business_month
        , sum(order_count) as order_count
        , sum(error_count) as error_count
        , 1.0 * sum(error_count) / sum(order_count) as error_rate
    from "torder".daily_stat 
    group by store_code, to_date(to_char(business_day, 'yyyy-mm-dd'), 'yyyy-mm')
);