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
    return p2p.check_data_exists(f"SELECT COUNT(*) FROM carga_de_esterilizador_liberada WHERE processed = False")

def decide_task_branch(ti):
    result = ti.xcom_pull(task_ids='check_data_exists_task')
    if result:
        return 'comisionado'
    else:
        return 'my_task_function'

def comisionado():
    time.sleep(90)
    kits = p2p.get_from_database("carga_de_esterilizador_liberada", "Carga de Esterilizador Liberada", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Comisionado"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed))

    p2p.save_to_database("comisionado", kits_mod) 
    p2p.process("carga_de_esterilizador_liberada", kits)  

with DAG(
    'comisionado',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 16),
        'catchup': False,
    },
    schedule_interval='*/23 * * * *',
    tags=['comisionado']
) as comisionado_dag:

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
        task_id="comisionado",
        python_callable=comisionado,
        provide_context=True
    )

    skip_downstream_tasks = PythonOperator(
        task_id="my_task_function",
        python_callable=my_task_function,
        provide_context=True
    )

    check_data_exists_task >> decide_task_branch_task
    decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]
