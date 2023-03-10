import requests
import json
import datetime
from datetime import date
from datetime import timedelta
import time
import psycopg2

from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.xcom import XCom

api_conn = BaseHook.get_connection('create_files_api')
url_conn = BaseHook.get_connection('url')
base_url = url_conn.host
method_url_couriers = '/couriers'
method_url_deliveries = '/deliveries'
dt = str(date.today() - timedelta(days=1))
exec_date = datetime.datetime.strptime(str(date.today()), '%Y-%m-%d').strftime('%Y-%m-%d 00:00:00')
pre_exec_date = datetime.datetime.strptime(str(date.today()-timedelta(days=1)), '%Y-%m-%d').strftime('%Y-%m-%d 00:00:00')

headers = {
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",  # ключ API
    "X-Nickname": "rover001koroteev",  # авторизационные данные
    "X-Cohort": "8"  # авторизационные данные
}


def load_stg_table_couriers(ti, url, method_url, headers):
    pg_schema = 'stg'
    pg_table = 'couriers'
    dwh_hook = PostgresHook('PG_WAREHOUSE_CONNECTION')
    conn = dwh_hook.get_conn()
    cur = conn.cursor()
    offset = 0
    while True:
        couriers_rep = requests.get(f'https://{base_url}/couriers/?sort_field=_id&sort_direction=asc&offset={offset}',
                                    headers=headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cur.close()
            conn.close()
            break

        columns = ','.join([i for i in couriers_rep[0]])
        values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]

        sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s ON CONFLICT (_id) DO NOTHING"
        execute_values(cur, sql, values)

        offset += len(couriers_rep)


def load_stg_table_deliveries(ti, url, method_url, headers):
    pg_schema = 'stg'
    pg_table = 'deliveries'
    dwh_hook = PostgresHook('PG_WAREHOUSE_CONNECTION')
    conn = dwh_hook.get_conn()
    cur = conn.cursor()

    offset = 0
    while True:
        deliveries_rep = requests.get(f'https://{base_url}/deliveries/?from={pre_exec_date}&to={exec_date}&'
                                      f'sort_field=_id&sort_direction=asc&offset={offset}',
                                    headers=headers).json()

        if len(deliveries_rep) == 0:
            conn.commit()
            cur.close()
            conn.close()
            break

        columns = ','.join([i for i in deliveries_rep[0]])
        values = [[value for value in deliveries_rep[i].values()] for i in range(len(deliveries_rep))]

        sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s ON CONFLICT (delivery_id) DO NOTHING"
        execute_values(cur, sql, values)

        offset += len(deliveries_rep)


def load_dds_dm():
    dwh_hook = PostgresHook('PG_WAREHOUSE_CONNECTION')
    conn = dwh_hook.get_conn()
    cur = conn.cursor()

    load_dm_couriers_dds = 'INSERT INTO dds.dm_couriers (courier_id_source, name)\
        SELECT _id, name\
        FROM stg.couriers\
        ON CONFLICT (courier_id_source) DO NOTHING;'
    cur.execute(load_dm_couriers_dds)
    conn.commit()


    load_dm_orders_dds = f"INSERT INTO dds.dm_orders (order_id_source)\
            SELECT order_id\
            FROM stg.deliveries \
            WHERE order_ts >= '{dt}'::date\
            ON CONFLICT (order_id_source) DO NOTHING;"
    cur.execute(load_dm_orders_dds)
    conn.commit()

    load_dm_deliveries_dds = f"INSERT INTO dds.dm_deliveries (delivery_id_source)\
                SELECT delivery_id\
                FROM stg.deliveries \
                WHERE order_ts >= '{dt}'::date\
                ON CONFLICT (delivery_id_source) DO NOTHING;"
    cur.execute(load_dm_deliveries_dds)
    conn.commit()
    cur.close()
    conn.close()


def load_dds_fct():
    dwh_hook = PostgresHook('PG_WAREHOUSE_CONNECTION')
    conn = dwh_hook.get_conn()
    cur = conn.cursor()

    load_fct_deliveries_dds = f"INSERT INTO dds.fct_deliveries (order_id_dwh, delivery_id_dwh, courier_id_dwh,\
        order_ts, delivery_ts, address, rate, tip_sum, total_sum)\
        SELECT order_id_dwh, delivery_id_dwh, courier_id_dwh, order_ts, delivery_ts, address, rate, tip_sum, sum \
        FROM stg.deliveries\
        inner join dds.dm_couriers on deliveries.courier_id = dm_couriers.courier_id_source\
        inner join dds.dm_deliveries on deliveries.delivery_id = dm_deliveries.delivery_id_source\
        inner join dds.dm_orders on deliveries.order_id = dm_orders.order_id_source\
        WHERE order_ts >= '{dt}'::date\
        ON CONFLICT (order_id_dwh) DO NOTHING;"
    cur.execute(load_fct_deliveries_dds)
    conn.commit()
    cur.close()
    conn.close()


def load_cdm():
    dwh_hook = PostgresHook('PG_WAREHOUSE_CONNECTION')
    conn = dwh_hook.get_conn()
    cur = conn.cursor()
    fd = "with agg_total as (\
    SELECT courier_id_dwh as courier_id,\
        date_part('year',order_ts::date) as settlement_year,\
        date_part('month',order_ts::date) as settlement_month,\
        avg(rate::numeric(14,2)) as rate_avg,\
        sum(total_sum) as orders_total_sum,\
        count(distinct(order_id_dwh)) as orders_count,\
        sum(total_sum * 0.25) as order_processing_fee,\
        sum(tip_sum) as courier_tips_sum\
    FROM dds.fct_deliveries\
    group by courier_id, settlement_year, settlement_month\
    )\
    ,agg_by_order as (\
    SELECT courier_id_dwh as courier_id,\
        order_id_dwh as order_id,\
        date_part('year',order_ts::date) as settlement_year,\
        date_part('month',order_ts::date) as settlement_month,\
        sum(total_sum) as orders_total_sum,\
        sum(total_sum * 0.25) as order_processing_fee,\
        sum(tip_sum) as courier_tips_sum\
    FROM dds.fct_deliveries\
    group by courier_id, \
        order_id_dwh, \
        settlement_year, \
        settlement_month\
    )\
    ,agg_order_sum as (\
    SELECT courier_id_dwh as courier_id,\
    date_part('year',order_ts::date) as settlement_year,\
    date_part('month',order_ts::date) as settlement_month,\
    sum(case WHEN agg_total.rate_avg < 4 then \
            CASE WHEN agg_by_order.orders_total_sum * 0.05 < 100 THEN 100 ELSE agg_by_order.orders_total_sum * 0.05 end\
        when agg_total.rate_avg >= 4 and agg_total.rate_avg < 4.5 then \
            CASE WHEN agg_by_order.orders_total_sum * 0.07 < 150 THEN 150 ELSE agg_by_order.orders_total_sum * 0.07 end\
        when agg_total.rate_avg >= 4.5 and agg_total.rate_avg < 4.9 then \
            CASE WHEN agg_by_order.orders_total_sum * 0.08 < 175 THEN 175 ELSE agg_by_order.orders_total_sum * 0.08 end\
        when agg_total.rate_avg >= 4.9 then \
            CASE WHEN agg_by_order.orders_total_sum * 0.1 < 200 THEN 200 ELSE agg_by_order.orders_total_sum * 0.1 end\
        end) as courier_order_sum\
    FROM dds.fct_deliveries\
    inner join agg_total \
        on fct_deliveries.courier_id_dwh = agg_total.courier_id \
        and date_part('year',fct_deliveries.order_ts::date) = agg_total.settlement_year \
        and date_part('month',fct_deliveries.order_ts::date) = agg_total.settlement_month \
    inner join agg_by_order \
        on fct_deliveries.courier_id_dwh = agg_by_order.courier_id \
        and fct_deliveries.order_id_dwh = agg_by_order.order_id\
        and date_part('year',fct_deliveries.order_ts::date) = agg_by_order.settlement_year \
        and date_part('month',fct_deliveries.order_ts::date) = agg_by_order.settlement_month \
    group by fct_deliveries.courier_id_dwh, \
            date_part('year',order_ts::date), \
            date_part('month',order_ts::date)\
    )\
    INSERT INTO cdm.dm_courier_ledger(courier_id,\
        courier_name,\
        settlement_year,\
        settlement_month,\
        orders_count,\
        orders_total_sum,\
        rate_avg,\
        order_processing_fee,\
        courier_order_sum,\
        courier_tips_sum,\
        courier_reward_sum)\
    SELECT distinct fct_deliveries.courier_id_dwh as courier_id, \
        dm_couriers.name as courier_name,\
        date_part('year',order_ts::date) as settlement_year,\
        date_part('month',order_ts::date) as settlement_month,\
        agg_total.orders_count as orders_count,\
        agg_total.orders_total_sum as orders_total_sum,\
        agg_total.rate_avg as rate_avg,\
        agg_total.order_processing_fee as order_processing_fee,\
        agg_order_sum.courier_order_sum as courier_order_sum,\
        agg_total.courier_tips_sum as courier_tips_sum,\
        agg_order_sum.courier_order_sum + agg_total.courier_tips_sum * 0.95 as courier_reward_sum\
    FROM dds.fct_deliveries\
    inner join dds.dm_couriers \
        on fct_deliveries.courier_id_dwh = dm_couriers.courier_id_dwh \
    left join agg_order_sum\
        on fct_deliveries.courier_id_dwh = agg_order_sum.courier_id\
        and date_part('year',fct_deliveries.order_ts::date) = agg_order_sum.settlement_year \
        and date_part('month',fct_deliveries.order_ts::date) = agg_order_sum.settlement_month \
    left join agg_total\
        on fct_deliveries.courier_id_dwh = agg_total.courier_id\
        and date_part('year',fct_deliveries.order_ts::date) = agg_total.settlement_year \
        and date_part('month',fct_deliveries.order_ts::date) = agg_total.settlement_month\
        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE\
        SET orders_count = EXCLUDED.orders_count,\
            orders_total_sum = EXCLUDED.orders_total_sum,\
            rate_avg = EXCLUDED.rate_avg,\
            order_processing_fee = EXCLUDED.order_processing_fee,\
            courier_order_sum = EXCLUDED.courier_order_sum,\
            courier_tips_sum = EXCLUDED.courier_tips_sum,\
            courier_reward_sum = EXCLUDED.courier_reward_sum;"

    cur.execute(fd)
    conn.commit()
    cur.close()
    conn.close()


dag = DAG(
    dag_id='load_stg_couriers',
    schedule_interval='0 1 * * *',
    start_date=datetime.datetime(2023, 2, 1),
    catchup=False,
    tags=['sprint5', 'project', 'couriers'],
    dagrun_timeout=datetime.timedelta(minutes=60)
)

t_stg_table_couriers = PythonOperator(task_id='load_stg_table_couriers',
                             python_callable=load_stg_table_couriers,
                             op_kwargs={'url': base_url,
                                        'method_url': method_url_couriers,
                                        'headers': headers
                                        },
                             dag=dag)

t_stg_table_deliveries = PythonOperator(task_id='load_stg_table_deliveries',
                             python_callable=load_stg_table_deliveries,
                             op_kwargs={'url': base_url,
                                        'method_url': method_url_deliveries,
                                        'headers': headers
                                        },
                             dag=dag)

t_dds_table_dm = PythonOperator(task_id='load_dds_table_dm',
                             python_callable=load_dds_dm,
                             dag=dag)

t_dds_table_fct = PythonOperator(task_id='load_dds_table_fct',
                             python_callable=load_dds_fct,
                             dag=dag)

t_cdm_table_fct = PythonOperator(task_id='load_cdm_table_fct',
                             python_callable=load_cdm,
                             dag=dag)

(t_stg_table_couriers, t_stg_table_deliveries) >> t_dds_table_dm >> t_dds_table_fct >> t_cdm_table_fct