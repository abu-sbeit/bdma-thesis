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
    return p2p.check_data_exists(f"SELECT COUNT(*) FROM produccion_montada WHERE processed = False")

def decide_task_branch(ti):
    result = ti.xcom_pull(task_ids='check_data_exists_task')
    if result:
        return 'composicion_de_cargas'
    else:
        return 'my_task_function'

def composicion_de_cargas():
    time.sleep(10)
    kits = p2p.get_from_database("produccion_montada", "Produccion Montada", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    esterilizadors = ["Autoclave 1", "Autoclave 2", "Autoclave 3", "Autoclave 4", "Jupiter"]
    activity_name = "Composicion de Cargas"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        esterilizador = random.choice(esterilizadors)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed, sterilization_machine=esterilizador))

    p2p.save_to_database("composicion_de_cargas", kits_mod) 
    p2p.process("produccion_montada", kits)  

with DAG(
    'composicion_de_cargas',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 16),
        'catchup': False,
    },
    schedule_interval='*/19 * * * *',
    tags=['composicion_de_cargas']
) as composicion_de_cargas_dag:

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
        task_id="composicion_de_cargas",
        python_callable=composicion_de_cargas,
        provide_context=True
    )

    skip_downstream_tasks = PythonOperator(
        task_id="my_task_function",
        python_callable=my_task_function,
        provide_context=True
    )

    check_data_exists_task >> decide_task_branch_task
    decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]
