create table if not exists "torder".daily_stat_overview as (
    select business_day
        , sum(order_count) as order_count_all
        , sum(error_count) as error_count_all 
        , 1.0 * sum(error_count) / sum(order_count) as error_rate_all
    from "torder".daily_stat 
    group by business_day
    order by business_day
);   