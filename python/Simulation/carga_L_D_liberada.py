from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import datetime
import py2postgres as p2p
import random
import time
from EventRecord import EventRecord as er

def my_task_function():
    time.sleep(20)
    print("Task completed after 100 seconds.")

def check_data_exists():
    return p2p.check_data_exists(f"SELECT COUNT(*) FROM carga_L_D_iniciada WHERE processed = False")

def decide_task_branch(ti):
    result = ti.xcom_pull(task_ids='check_data_exists_task')
    if result:
        return 'carga_L-D_liberada'
    else:
        return 'my_task_function'

def carga_L_D_liberada():
    time.sleep(90)
    kits = p2p.get_from_database("carga_L_D_iniciada", "Carga L+D Iniciada", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Carga L+D Liberada"
    kits_mod = []
    for i in range(len(kits)):
        kit = kits[i]
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits_mod.append(er(kit.kit_id, timestamp, resource, activity_name, quantity, processed, rack=kit.rack, washing_machine=kit.washing_machine))

    p2p.save_to_database("carga_L_D_liberada", kits_mod) 
    p2p.process("carga_L_D_iniciada", kits)  

with DAG(
    'carga_L_D_liberada',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 16),
        'catchup': False,
    },
    schedule_interval='*/13 * * * *',
    tags=['carga_L_D_liberada']
) as carga_L_D_liberada_dag:

    check_data_exists_task = PythonOperator(
    task_id='check_data_exists_task',
    python_callable=check_data_exists
)

    decide_task_branch_task = BranchPythonOperator(
    task_id='decide_task_branch_task',
    python_callable=decide_task_branch,
    provide_context=True
)

    execute_downstream_tasks = PythonOperator(
        task_id="carga_L-D_liberada",
        python_callable=carga_L_D_liberada,
        provide_context=True
    )

    skip_downstream_tasks = PythonOperator(
        task_id="my_task_function",
        python_callable=my_task_function,
        provide_context=True
    )

    check_data_exists_task >> decide_task_branch_task
    decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]
