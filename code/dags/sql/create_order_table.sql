create table "torder".orders (
    id int4 NULL,
    store_code varchar(50) NULL,
    time_order varchar(50) NULL,
    is_paid_order bool NULL,
    is_first_order bool NULL,
    is_error_exists bool NULL
);