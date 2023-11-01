create table if not exists "torder".daily_stat_by_store as (
    select store_code
        , business_day
        , order_count
        , error_count
        , 1.0 * error_count / order_count as error_rate
    from "torder".daily_stat
);