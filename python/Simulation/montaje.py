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
    return p2p.check_data_exists(f"SELECT COUNT(*) FROM carga_L_D_liberada WHERE processed = False")

def decide_task_branch(ti):
    result = ti.xcom_pull(task_ids='check_data_exists_task')
    if result:
        return 'montaje'
    else:
        return 'my_task_function'

def montaje():
    records = p2p.get_from_database("carga_L_D_liberada", "Carga L+D Liberada", 10)
    i = 0
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Montaje"
    processed = False
    while (i < len(records)):
        time.sleep(random.randint(0, 4))
        record = records[i]
        kit_id = record.kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        if (kit_id.startswith("CONT-")):
            container = p2p.get_containers(f"SELECT * FROM containers where id = '{kit_id}'")[0]
            kits_in_container = p2p.get_kits(f"SELECT * FROM kits where isContainedKit = true limit {container.numOfKitsContained}")
            for _ in range(len(kits_in_container)):
                kit= random.choice(kits_in_container)
                kit_id = kit.id
                quantity = kit.count
                timestamp = datetime.now()
                montaje_kit = er(kit_id, timestamp, resource, activity_name, quantity, processed)
                p2p.save_record_to_database("montaje", montaje_kit)
                for _ in range(quantity):
                    timestamp = datetime.now()
                    quantity = 1
                    kits_mod = []
                    produccion_montada = "Produccion Montada"
                    kits_mod.append(er(kit_id, timestamp, resource, produccion_montada, quantity, processed))
                p2p.save_to_database("produccion_montada", kits_mod) 
                p2p.process_record_using_activity_name_kit_id("montaje", montaje_kit)
        else:
            kit = p2p.get_kits(f"SELECT * FROM kits where id = '{kit_id}'")[0]
            quantity = kit.count
            montaje_record = records[i]
            montaje_record.quantity = quantity
            p2p.modify_record("montaje", montaje_record)
            for _ in range(quantity):
                timestamp = datetime.now()
                quantity = 1
                kits_mod = []
                produccion_montada = "Produccion Montada"
                kits_mod.append(er(kit_id, timestamp, resource, produccion_montada, quantity, processed))
            p2p.save_to_database("produccion_montada", kits_mod) 
            p2p.process_record("montaje", montaje_record)
        i = i+1

with DAG(
    'montaje',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 16),
        'catchup': False,
    },
    schedule_interval='*/15 * * * *',
    tags=['montaje']
) as montaje_dag:

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
        task_id="montaje",
        python_callable=montaje,
        provide_context=True
    )

    skip_downstream_tasks = PythonOperator(
        task_id="my_task_function",
        python_callable=my_task_function,
        provide_context=True
    )

    check_data_exists_task >> decide_task_branch_task
    decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]
