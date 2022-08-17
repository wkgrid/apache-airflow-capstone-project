
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


#
config = {
    'dag_id_1': {'schedule_interval': "", 'start_date' : datetime(2020, 1, 1,)},
    'dag_id_2': {'schedule_interval': "", 'start_date' : datetime(2020, 1, 1,)},
    'dag_id_3': {'schedule_interval': "", 'start_date' : datetime(2020, 1, 1,)},
}

print('start')

############################################

with DAG(
        "lab1",                      # (Dag id) NOTE: name change is not reflected on the UI after page refresh
        start_date=datetime(2021, 1, 1),    # start date, the 1st of January 2021
        schedule_interval='@daily',         # Cron expression, here it is a preset of Airflow, @daily means once every day.
        catchup=False                       # Catchup
) as dag1:
   task_a = EmptyOperator(task_id="task_a")
   task_b = EmptyOperator(task_id="task_b")
   task_c = EmptyOperator(task_id="task_c")
   task_d = EmptyOperator(task_id="task_d")
   task_a >> [task_b, task_c]               # task_a is executed before b and c. Tasks b & c do not depend on each other
   task_c >> task_d                         # task_c is executed before d

#############################################

with DAG(
        "lab2_fake_db_actions",      # Dag id
        start_date=datetime(2021, 1, 1),    # start date, the 1st of January 2021
        schedule_interval='@daily',         # Cron expression, here it is a preset of Airflow, @daily means once every day.
        catchup=False                       # Catchup
) as dag2:
   insert_row_task = EmptyOperator(task_id="insert_row")    # NOTE: No spaces and hyphens in the name
   table_query_task = EmptyOperator(task_id="query_table")
   insert_row_task >> table_query_task


#############################################


with DAG(
        "lab3_branch",
        start_date=datetime(2021, 1, 1),    # start date, the 1st of January 2021
        schedule_interval='@daily',         # Cron expression, here it is a preset of Airflow, @daily means once every day.
        catchup=False                       # Catchup
) as dag3:

    def check_table_exists():
        if (True):
            return "insert_row"
        else:
            return "create_table"

    check_table_exists_task = BranchPythonOperator(task_id="check_if_table_exists", python_callable=check_table_exists)
    insert_row_task = EmptyOperator(task_id="insert_row", trigger_rule='none_failed')
    create_table_task = EmptyOperator(task_id="create_table")
    table_query_task = EmptyOperator(task_id="query_table")
    check_table_exists_task >> [create_table_task, insert_row_task]
    create_table_task >> insert_row_task
    insert_row_task >> table_query_task


#############################################

from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
        "lab4_trigger",                     #
        start_date=datetime(2021, 1, 1),    # start date, the 1st of January 2021
        schedule_interval='@daily',         # Cron expression, here it is a preset of Airflow, @daily means once every day.
        catchup=False                       # Catchup
) as dag4:

    file_path = "/home/wojtek/Workspace/max-ml/big-data/airflow/tests/file.txt"

    file_sensor_task = FileSensor(task_id="file_sensor", filepath=file_path)
    trigger_dag_task = TriggerDagRunOperator(task_id="trigger_dag", trigger_dag_id="lab3_branch")
    remove_file_task = BashOperator(task_id="remove_file", bash_command=f'rm {file_path}; echo REMOVED')
    file_sensor_task >> trigger_dag_task >> remove_file_task
