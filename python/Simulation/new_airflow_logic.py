from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import datetime
from airflow.models import Variable
import py2postgres as p2p
import random
import time
from EventRecord import EventRecord as er
from EventRecord import TaskExecution as te
from Kit import Kit as kit

def my_task_function():
    time.sleep(20)
    print("Task completed after 100 seconds.")

def check_data_exists(**kwargs):
    task = kwargs['task']
    data = p2p.check_data_exists(f"SELECT COUNT(*) FROM {task} WHERE processed = False")
    return te(task, data)

def decide_task_branch(ti):
    result = ti.xcom_pull(task_ids='check_data_exists_task')
    if result.task == "entrada_material_sucio":
        return 'entrada_material_sucio'
    elif result.task == "cargado_en_carro_L_D":
        if result.data:
            return 'cargado_en_carro_L_D'
        else:
            return 'my_task_function'
    elif result.task == "carga_L_D_iniciada":
        if result.data:
            return 'carga_L_D_iniciada'
        else:
            return 'my_task_function'
    elif result.task == "carga_L_D_liberada":
        if result.data:
            return 'carga_L_D_liberada'
        else:
            return 'my_task_function'
    elif result.task == "montaje":
        if result.data:
            return 'montaje'
        else:
            return 'my_task_function'
    elif result.task == "composicion_de_cargas":
        if result.data:
            return 'composicion_de_cargas'
        else:
            return 'my_task_function'
    elif result.task == "carga_de_esterilizador_liberada":
        if result.data:
            return 'carga_de_esterilizador_liberada'
        else:
            return 'my_task_function'

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
            container = p2p.get_containers(f"SELECT * FROM containers where id = {kit_id}")[0]
            kits_in_container = p2p.get_kits(f"SELECT * FROM kits where isContainedKit = true limit {container.numOfKitsContained}")
            for _ in range(len(kits_in_container)):
                kit= random.choice(kits_in_container)
                kit_id = kit.kit_id
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
            kit = p2p.get_kits(f"SELECT * FROM kits where id = {kit_id}")[0]
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

def carga_de_esterilizador_liberada():
    time.sleep(90)
    kits = p2p.get_from_database("composicion_de_cargas", "Composicion de Cargas", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Carga de Esterilizador Liberada"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed))

    p2p.save_to_database("carga_de_esterilizador_liberada", kits_mod) 
    p2p.process("composicion_de_cargas", kits)  

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

# with DAG(
#     'entrada_material_sucio',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'catchup': False,
#     },
#     schedule_interval='*/5 * * * *',
#     tags=['entrada_material_sucio']
# ) as entrada_material_sucio_dag:

#     check_data_exists_task = PythonOperator(
#     task_id='check_data_exists_task',
#     python_callable=check_data_exists,
#     op_kwargs={'task': 'entrada_material_sucio'}
# )

#     decide_task_branch_task = BranchPythonOperator(
#     task_id='decide_task_branch_task',
#     python_callable=decide_task_branch,
#     provide_context=True
# )

#     skip_downstream_tasks = PythonOperator(
#         task_id="entrada_material_sucio",
#         python_callable=entrada_material_sucio,
#         provide_context=True
#     )

#     check_data_exists_task >> decide_task_branch_task
#     decide_task_branch_task >> skip_downstream_tasks

# with DAG(
#     'cargado_en_carro_L_D',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'catchup': False,
#     },
#     schedule_interval='*/7 * * * *',
#     tags=['cargado_en_carro_L_D']
# ) as cargado_en_carro_L_D_dag:

#     check_data_exists_task = PythonOperator(
#     task_id='check_data_exists_task',
#     python_callable=check_data_exists,
#     op_kwargs={'task': 'cargado_en_carro_L_D'}
# )

#     decide_task_branch_task = BranchPythonOperator(
#     task_id='decide_task_branch_task',
#     python_callable=decide_task_branch,
#     provide_context=True
# )

#     execute_downstream_tasks = PythonOperator(
#         task_id="cargado_en_carro_L-D",
#         python_callable=cargado_en_carro_L_D,
#         provide_context=True
#     )

#     skip_downstream_tasks = PythonOperator(
#         task_id="my_task_function",
#         python_callable=my_task_function,
#         provide_context=True
#     )

#     check_data_exists_task >> decide_task_branch_task
#     decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]

# with DAG(
#     'carga_L_D_iniciada',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'catchup': False,
#     },
#     schedule_interval='*/10 * * * *',
#     tags=['carga_L_D_iniciada']
# ) as carga_L_D_iniciada_dag:

#     check_data_exists_task = PythonOperator(
#     task_id='check_data_exists_task',
#     python_callable=check_data_exists,
#     op_kwargs={'task': 'carga_L_D_iniciada'}
# )

#     decide_task_branch_task = BranchPythonOperator(
#     task_id='decide_task_branch_task',
#     python_callable=decide_task_branch,
#     provide_context=True
# )

#     execute_downstream_tasks = PythonOperator(
#         task_id="carga_L-D_iniciada",
#         python_callable=carga_L_D_iniciada,
#         provide_context=True
#     )

#     skip_downstream_tasks = PythonOperator(
#         task_id="my_task_function",
#         python_callable=my_task_function,
#         provide_context=True
#     )

#     check_data_exists_task >> decide_task_branch_task
#     decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]

# with DAG(
#     'carga_L_D_liberada',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'catchup': False,
#     },
#     schedule_interval='*/13 * * * *',
#     tags=['carga_L_D_liberada']
# ) as carga_L_D_liberada_dag:

#     check_data_exists_task = PythonOperator(
#     task_id='check_data_exists_task',
#     python_callable=check_data_exists,
#     op_kwargs={'task': 'carga_L_D_liberada'}
# )

#     decide_task_branch_task = BranchPythonOperator(
#     task_id='decide_task_branch_task',
#     python_callable=decide_task_branch,
#     provide_context=True
# )

#     execute_downstream_tasks = PythonOperator(
#         task_id="carga_L-D_liberada",
#         python_callable=carga_L_D_liberada,
#         provide_context=True
#     )

#     skip_downstream_tasks = PythonOperator(
#         task_id="my_task_function",
#         python_callable=my_task_function,
#         provide_context=True
#     )

#     check_data_exists_task >> decide_task_branch_task
#     decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]

# with DAG(
#     'montaje',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'catchup': False,
#     },
#     schedule_interval='*/16 * * * *',
#     tags=['montaje']
# ) as montaje_dag:

#     check_data_exists_task = PythonOperator(
#     task_id='check_data_exists_task',
#     python_callable=check_data_exists,
#     op_kwargs={'task': 'montaje'}
# )

#     decide_task_branch_task = BranchPythonOperator(
#     task_id='decide_task_branch_task',
#     python_callable=decide_task_branch,
#     provide_context=True
# )

#     execute_downstream_tasks = PythonOperator(
#         task_id="montaje",
#         python_callable=montaje,
#         provide_context=True
#     )

#     skip_downstream_tasks = PythonOperator(
#         task_id="my_task_function",
#         python_callable=my_task_function,
#         provide_context=True
#     )

#     check_data_exists_task >> decide_task_branch_task
#     decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]

# with DAG(
#     'composicion_de_cargas',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'catchup': False,
#     },
#     schedule_interval='*/18 * * * *',
#     tags=['composicion_de_cargas']
# ) as composicion_de_cargas_dag:

#     check_data_exists_task = PythonOperator(
#     task_id='check_data_exists_task',
#     python_callable=check_data_exists,
#     op_kwargs={'task': 'composicion_de_cargas'}
# )

#     decide_task_branch_task = BranchPythonOperator(
#     task_id='decide_task_branch_task',
#     python_callable=decide_task_branch,
#     provide_context=True
# )

#     execute_downstream_tasks = PythonOperator(
#         task_id="composicion_de_cargas",
#         python_callable=composicion_de_cargas,
#         provide_context=True
#     )

#     skip_downstream_tasks = PythonOperator(
#         task_id="my_task_function",
#         python_callable=my_task_function,
#         provide_context=True
#     )

#     check_data_exists_task >> decide_task_branch_task
#     decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]

# with DAG(
#     'carga_de_esterilizador_liberada',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'catchup': False,
#     },
#     schedule_interval='*/22 * * * *',
#     tags=['carga_de_esterilizador_liberada']
# ) as carga_de_esterilizador_liberada_dag:

#     check_data_exists_task = PythonOperator(
#     task_id='check_data_exists_task',
#     python_callable=check_data_exists,
#     op_kwargs={'task': 'carga_de_esterilizador_liberada'}
# )

#     decide_task_branch_task = BranchPythonOperator(
#     task_id='decide_task_branch_task',
#     python_callable=decide_task_branch,
#     provide_context=True
# )

#     execute_downstream_tasks = PythonOperator(
#         task_id="carga_de_esterilizador_liberada",
#         python_callable=carga_de_esterilizador_liberada,
#         provide_context=True
#     )

#     skip_downstream_tasks = PythonOperator(
#         task_id="my_task_function",
#         python_callable=my_task_function,
#         provide_context=True
#     )

#     check_data_exists_task >> decide_task_branch_task
#     decide_task_branch_task >> [execute_downstream_tasks, skip_downstream_tasks]
