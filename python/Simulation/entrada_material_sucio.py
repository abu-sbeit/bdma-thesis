from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from EventRecord import EventRecord as er
import py2postgres as p2p
import random

def entrada_material_sucio():
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Entrada Material Sucio"
    kits = p2p.get_kits("SELECT * FROM kits where isContainedKit = false limit 70")
    kits = kits + p2p.get_containers("SELECT * FROM containers limit 30")
    entrada_material_sucio_list = [] 
    for _ in range(20):
        kit = random.choice(kits)
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        entrada_material_sucio_list.append(er(kit.id, timestamp, resource, activity_name, quantity, processed))
    p2p.save_to_database("entrada_material_sucio", entrada_material_sucio_list)    

with DAG(
    'entrada_material_sucio',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 16),
        'catchup': False,
    },
    schedule_interval='*/5 * * * *',
    tags=['entrada_material_sucio']
) as entrada_material_sucio_dag:

    entrada_material_sucio_task = PythonOperator(
        task_id="entrada_material_sucio",
        python_callable=entrada_material_sucio,
        provide_context=True
    )

    entrada_material_sucio_task
