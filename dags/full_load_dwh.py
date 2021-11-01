from datetime import datetime

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False)
def full_load_dwh():
    dimensions_dag = TriggerDagRunOperator(
        task_id="dimensions_task",
        trigger_dag_id="dimensions",
        wait_for_completion=True,
        poke_interval=10,
    )
    facts_dag = TriggerDagRunOperator(
        task_id="facts_task", trigger_dag_id="facts", wait_for_completion=True
    )

    dimensions_dag >> facts_dag


full_load_dwh_dag = full_load_dwh()
