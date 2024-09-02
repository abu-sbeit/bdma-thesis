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
    return p2p.check_data_exists(f"SELECT COUNT(*) FROM cargado_en_carro_L_D WHERE processed = False")

def decide_task_branch(ti):
    result = ti.xcom_pull(task_ids='check_data_exists_task')
    if result:
        return 'carga_L-D_iniciada'
    else:
        return 'my_task_function'

def carga_L_D_iniciada():
    time.sleep(10)
    kits = p2p.get_from_database("cargado_en_carro_L_D", "Cargado en Carro L+D", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    lavadoras = ["Lavadora 1", "Lavadora 2", "Lavadora 3", "Lavadora 4", "Jupiter"]
    activity_name = "Carga L+D Iniciada"
    kits_mod = []
    for i in range(len(kits)):
        kit = kits[i]
        timestamp = datetime.now()
        resource = random.choice(resources)
        lavadora = random.choice(lavadoras)
        quantity = 1
        processed = False
        kits_mod.append(er(kit.kit_id, timestamp, resource, activity_name, quantity, processed, rack=kit.rack, washing_machine=lavadora))

    p2p.save_to_database("carga_L_D_iniciada", kits_mod) 
    p2p.process("cargado_en_carro_L_D", kits) 

with DAG(
    'carga_L_D_iniciada',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 16),
        'catchup': False,
    },
    schedule_interval='*/10 * * * *',
    tags=['carga_L_D_iniciada']
) as carga_L_D_iniciada_dag:

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
        task_id="carga_L-D_iniciada",
        python_callable=carga_L_D_iniciada,
        provide_context=True
    )

    skip_downstream_tasks = PythonOperator(
        task_id="my_task_function",
        python_callable=my_task_function,
        provide_context=True
    )

    check_data_exists_task >> decide_task_branch_task
    decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]
