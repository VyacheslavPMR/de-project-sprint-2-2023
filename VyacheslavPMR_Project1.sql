DROP TABLE IF EXISTS dwh.customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id BIGINT NOT NULL,
    customer_name VARCHAR NOT NULL,
    customer_address VARCHAR NOT NULL,
    customer_birthday DATE NOT NULL,
    customer_email VARCHAR NOT NULL,
    total_spent NUMERIC(15,2) NOT NULL,
    platform_revenue NUMERIC(15,2) NOT NULL,
    order_count INT NOT NULL,
    average_order_value NUMERIC(10,2) NOT NULL,
    favorite_category VARCHAR NOT NULL,
    favorite_craftsman_id BIGINT,
    median_days_to_completion NUMERIC(10,1),
    orders_created INT,
    orders_in_progress INT NOT NULL,
    orders_in_delivery INT NOT NULL,
    orders_completed INT NOT NULL,
    orders_not_completed INT NOT NULL,
    report_period VARCHAR NOT NULL,
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);


-- DDL таблицы инкрементальных загрузок для отчётов по заказчикам
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);

WITH dwh_delta AS (
    SELECT
        dc.customer_id AS customer_id,
        dc.customer_name AS customer_name,
        dc.customer_address AS customer_address,
        dc.customer_birthday AS customer_birthday,
        dc.customer_email AS customer_email,
        fo.order_id AS order_id,
        dp.product_id AS product_id,
        dp.product_price AS product_price,
        dp.product_type AS product_type,
        DATE_PART('year', AGE(dc.customer_birthday)) AS customer_age,
        fo.order_completion_date - fo.order_created_date AS diff_order_date,
        fo.order_status AS order_status,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        crd.customer_id AS exist_customer_id,
        dc.load_dttm AS customers_load_dttm,
        dcs.load_dttm AS craftsman_load_dttm,
        dp.load_dttm AS products_load_dttm,
		dcs.craftsman_id AS craftsman_id
    FROM dwh.f_order fo
    INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id
    INNER JOIN dwh.d_craftsman dcs ON fo.craftsman_id = dcs.craftsman_id
    INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
    LEFT JOIN dwh.customer_report_datamart crd ON dc.customer_id = crd.customer_id
    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),

dwh_update_delta AS ( -- делаем выборку заказчиков, по которым были изменения в DWH. По этим заказчикам данные в витрине нужно будет обновить
    SELECT
            dd.exist_customer_id AS customer_id
    FROM dwh_delta dd
    WHERE dd.exist_customer_id IS NOT NULL
),

dwh_delta_insert_result as (
SELECT
            T4.customer_id AS customer_id,
            T4.customer_name AS customer_name,
            T4.customer_address AS customer_address,
            T4.customer_birthday AS customer_birthday,
            T4.customer_email AS customer_email,
            T4.customer_money AS customer_money,
            T4.platform_money AS platform_money,
            T4.count_order AS count_order,
            T4.avg_price_order AS avg_price_order,
            T4.product_type AS top_product_category,
            T4.craftsman_id as top_craftsman,
            T4.median_time_order_completed AS median_time_order_completed,
            T4.count_order_created AS count_order_created,
            T4.count_order_in_progress AS count_order_in_progress,
            T4.count_order_delivery AS count_order_delivery,
            T4.count_order_done AS count_order_done,
            T4.count_order_not_done AS count_order_not_done,
            T4.report_period AS report_period
FROM

(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY count_craftsman DESC) AS rank_count_craftman 

FROM
(
SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
T1.customer_id AS customer_id, -- идентификатор заказчика;
T1.customer_name AS customer_name, -- Ф. И. О. заказчика;
T1.customer_address AS customer_address, -- адрес заказчика;
T1.customer_birthday AS customer_birthday, -- дата рождения заказчика;
T1.customer_email AS customer_email, -- электронная почта заказчика;
SUM(T1.product_price) AS customer_money, -- сумма, которую потратил заказчик;
SUM(T1.product_price) * 0.1 AS platform_money, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик);
COUNT(order_id) AS count_order, -- количество заказов у заказчика за месяц;
AVG(T1.product_price) AS avg_price_order, -- средняя стоимость одного заказа у заказчика за месяц;
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY T1.diff_order_date) AS median_time_order_completed, -- медианное время в днях от момента создания заказа до его завершения за месяц;
SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, -- количество созданных заказов за месяц;
SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, -- количество заказов в процессе изготовки за месяц;
SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, -- количество заказов в доставке за месяц;
SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, -- количество завершённых заказов за месяц;
SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done, -- количество незавершённых заказов за месяц;
T1.report_period AS report_period -- отчётный период, год и месяц.
FROM dwh_delta AS T1
WHERE T1.exist_customer_id IS NULL
GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period) AS T2

INNER JOIN 

(
SELECT     -- Эта выборка поможет определить самый популярный товар у покупателя. Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. Для данных этой выборки можно применить оконную функцию, которая и покажет самую популярную категорию товаров у мастера
dd.customer_id AS customer_id_for_product_type, 
dd.product_type, 
COUNT(dd.product_id) AS count_product
FROM dwh_delta AS dd
GROUP BY dd.customer_id, dd.product_type
ORDER BY count_product DESC) AS T3 ON T2.customer_id = T3.customer_id_for_product_type

INNER JOIN 

(
SELECT
dd.customer_id AS customer_id_for_top_craftsman,
dd.craftsman_id,
COUNT(dd.craftsman_id) AS count_craftsman
FROM dwh_delta AS dd
GROUP BY dd.customer_id, dd.craftsman_id
ORDER BY count_craftsman DESC) AS T5 ON T2.customer_id = T5.customer_id_for_top_craftsman) as T4

WHERE T4.rank_count_product = 1 AND T4.rank_count_craftman = 1
ORDER BY report_period
),

dwh_delta_update_result as(

SELECT
            T4.customer_id AS customer_id,
            T4.customer_name AS customer_name,
            T4.customer_address AS customer_address,
            T4.customer_birthday AS customer_birthday,
            T4.customer_email AS customer_email,
            T4.customer_money AS customer_money,
            T4.platform_money AS platform_money,
            T4.count_order AS count_order,
            T4.avg_price_order AS avg_price_order,
            T4.product_type AS top_product_category,
            T4.craftsman_id as top_craftsman,
            T4.median_time_order_completed AS median_time_order_completed,
            T4.count_order_created AS count_order_created,
            T4.count_order_in_progress AS count_order_in_progress,
            T4.count_order_delivery AS count_order_delivery,
            T4.count_order_done AS count_order_done,
            T4.count_order_not_done AS count_order_not_done,
            T4.report_period AS report_period
FROM

(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY count_craftsman DESC) AS rank_count_craftman 

FROM
(
SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
T1.customer_id AS customer_id, -- идентификатор заказчика;
T1.customer_name AS customer_name, -- Ф. И. О. заказчика;
T1.customer_address AS customer_address, -- адрес заказчика;
T1.customer_birthday AS customer_birthday, -- дата рождения заказчика;
T1.customer_email AS customer_email, -- электронная почта заказчика;
SUM(T1.product_price) AS customer_money, -- сумма, которую потратил заказчик;
SUM(T1.product_price) * 0.1 AS platform_money, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик);
COUNT(order_id) AS count_order, -- количество заказов у заказчика за месяц;
AVG(T1.product_price) AS avg_price_order, -- средняя стоимость одного заказа у заказчика за месяц;
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY T1.diff_order_date) AS median_time_order_completed, -- медианное время в днях от момента создания заказа до его завершения за месяц;
SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, -- количество созданных заказов за месяц;
SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, -- количество заказов в процессе изготовки за месяц;
SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, -- количество заказов в доставке за месяц;
SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, -- количество завершённых заказов за месяц;
SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done, -- количество незавершённых заказов за месяц;
T1.report_period AS report_period -- отчётный период, год и месяц.
FROM 

(
SELECT
        dc.customer_id AS customer_id,
        dc.customer_name AS customer_name,
        dc.customer_address AS customer_address,
        dc.customer_birthday AS customer_birthday,
        dc.customer_email AS customer_email,
        fo.order_id AS order_id,
        dp.product_id AS product_id,
        dp.product_price AS product_price,
        dp.product_type AS product_type,
        DATE_PART('year', AGE(dc.customer_birthday)) AS customer_age,
        fo.order_completion_date - fo.order_created_date AS diff_order_date,
        fo.order_status AS order_status,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        dc.load_dttm AS customer_load_dttm,
        dcs.load_dttm AS customers_load_dttm,
        dp.load_dttm AS products_load_dttm,
		dcs.craftsman_id AS craftsman_id
    FROM dwh.f_order fo
    INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id
    INNER JOIN dwh.d_craftsman dcs ON fo.craftsman_id = dcs.craftsman_id
    INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
    INNER JOIN dwh_update_delta ud ON fo.customer_id = ud.customer_id
)
AS T1
GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period) AS T2

INNER JOIN 

(
SELECT     -- Эта выборка поможет определить самый популярный товар у покупателя. Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. Для данных этой выборки можно применить оконную функцию, которая и покажет самую популярную категорию товаров у мастера
dd.customer_id AS customer_id_for_product_type, 
dd.product_type, 
COUNT(dd.product_id) AS count_product
FROM dwh_delta AS dd
GROUP BY dd.customer_id, dd.product_type
ORDER BY count_product DESC) AS T3 ON T2.customer_id = T3.customer_id_for_product_type

INNER JOIN 

(
SELECT
dd.customer_id AS customer_id_for_top_craftsman,
dd.craftsman_id,
COUNT(dd.craftsman_id) AS count_craftsman
FROM dwh_delta AS dd
GROUP BY dd.customer_id, dd.craftsman_id
ORDER BY count_craftsman DESC) AS T5 ON T2.customer_id = T5.customer_id_for_top_craftsman) as T4

WHERE T4.rank_count_product = 1 AND T4.rank_count_craftman = 1
ORDER BY report_period
),

insert_delta as (
INSERT INTO dwh.customer_report_datamart (
	customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email,
    total_spent,
    platform_revenue,
    order_count,
    average_order_value,
    favorite_category,
    favorite_craftsman_id,
    median_days_to_completion,
    orders_created,
    orders_in_progress,
    orders_in_delivery,
    orders_completed,
    orders_not_completed,
    report_period
    ) SELECT 
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money,
            platform_money,
            count_order,
            avg_price_order,
            top_product_category,
            top_craftsman,
            median_time_order_completed,
            count_order_created,
            count_order_in_progress,
            count_order_delivery,
            count_order_done,
            count_order_not_done,
            report_period
    FROM dwh_delta_insert_result
 ),
 
 update_delta AS ( -- выполняем обновление показателей в отчёте по уже существующим мастерам
    UPDATE dwh.customer_report_datamart SET
	customer_id = updates.customer_id,
    customer_name = updates.customer_name,
    customer_address = updates.customer_address,
    customer_birthday = updates.customer_birthday,
    customer_email = updates.customer_email,
    total_spent = updates.customer_money,
    platform_revenue = updates.platform_money,
    order_count = updates.count_order,
    average_order_value = updates.avg_price_order,
    favorite_category = updates.top_product_category,
    favorite_craftsman_id = updates.top_craftsman,
    median_days_to_completion = updates.median_time_order_completed,
    orders_created = updates.count_order_created,
    orders_in_progress = updates.count_order_in_progress,
    orders_in_delivery = updates.count_order_delivery,
    orders_completed = updates.count_order_done,
    orders_not_completed = updates.count_order_not_done,
    report_period = updates.report_period
    FROM (
        SELECT 
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money,
            platform_money,
            count_order,
            avg_price_order,
            top_product_category,
            top_craftsman,
            median_time_order_completed,
            count_order_created,
            count_order_in_progress,
            count_order_delivery,
            count_order_done,
            count_order_not_done,
            report_period
            FROM dwh_delta_update_result) AS updates
    WHERE dwh.customer_report_datamart.customer_id = updates.customer_id
),

insert_load_date AS ( -- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
        FROM dwh_delta
)

 select 'complete'