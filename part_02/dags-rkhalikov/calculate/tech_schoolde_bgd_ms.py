from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from beeline.airflow.hashicorp_vault.VaultOperator import VaultOperator
from airflow.operators.dummy_operator import DummyOperator
import pendulum

local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 12, 1, 0, tzinfo=local_tz),
    'email': ['rkhalikov@ ...'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'school_de_rkhalikov_calc',
    default_args=default_args,
    description='Calc results',
    schedule_interval=None,
    tags=['school_de_rkhalikov'],
)

dag.doc_md = """
#### Проект:
[SDE]
#### Ссылка на задачу:
[SD]
#### Ссылка на спарктаску:
#### Описание DAG:
Расчёт задач
#### Входные данные:
hive - school_de.bookings_ticket_flights
hive - school_de.bookings_flights
hive - school_de.bookings_flights_v
hive - school_de.bookings_airports
hive - school_de.bookings_routes
hive - school_de.bookings_aircrafts
#### Выходные данные:
hive - school_de.results_rkhalikov
"""

start_DAG = DummyOperator(
    task_id='start',
    dag=dag)

submit_spark_job = SparkSubmitOperator(
    application="hdfs://.../datamarts-rkhalikov-assembly-0.1.jar",
    name="school_de_rkhalikov_calc",
    conf={'spark.yarn.queue': 'prod', 'spark.submit.deployMode': 'cluster', 'spark.driver.memory': '4g',
          'spark.executor.memory': '8g', 'spark.executor.cores': '1', 'spark.dynamicAllocation.enabled': 'true',
          'spark.shuffle.service.enabled': 'true', 'spark.dynamicAllocation.maxExecutors': '10'
          },
    task_id="submit_rkhalikov_calc",
    conn_id="hdp31_spark",
    java_class="MainAppCalc",
    queue='prod',
    dag=dag
)
start_DAG >> submit_spark_job
