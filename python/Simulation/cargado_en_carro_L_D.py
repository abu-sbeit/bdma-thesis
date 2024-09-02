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
    return p2p.check_data_exists(f"SELECT COUNT(*) FROM entrada_material_sucio WHERE processed = False")

def decide_task_branch(ti):
    result = ti.xcom_pull(task_ids='check_data_exists_task')
    if result:
        return 'cargado_en_carro_L-D'
    else:
        return 'my_task_function'

def cargado_en_carro_L_D():
    time.sleep(10)
    kits = p2p.get_from_database("entrada_material_sucio", "Entrada Material Sucio", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    racks = ["Rack 1", "Rack 2", "Rack 3", "Rack 4", "Rack 5", "Rack 6", "Rack 7", "Rack 8", "Rack 9"]
    activity_name = "Cargado en Carro L+D"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        rack = random.choice(racks)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed, rack=rack))

    p2p.save_to_database("cargado_en_carro_L_D", kits_mod) 
    p2p.process("entrada_material_sucio", kits)  

with DAG(
    'cargado_en_carro_L_D',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 16),
        'catchup': False,
    },
    schedule_interval='*/7 * * * *',
    tags=['cargado_en_carro_L_D']
) as cargado_en_carro_L_D_dag:

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
        task_id="cargado_en_carro_L-D",
        python_callable=cargado_en_carro_L_D,
        provide_context=True
    )

    skip_downstream_tasks = PythonOperator(
        task_id="my_task_function",
        python_callable=my_task_function,
        provide_context=True
    )

    check_data_exists_task >> decide_task_branch_task
    decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]
