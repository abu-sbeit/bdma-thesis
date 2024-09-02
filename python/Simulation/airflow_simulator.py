from datetime import datetime, timedelta
import random
import time
import py2postgres as p2p
from EventRecord import EventRecord as er
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def entrada_material_sucio():
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Entrada Material Sucio"
    
    kits = []
    for _ in range(100):
        kit_id = random.choice(["CONT-", "HUBU-"]) + str(random.randint(1000, 9999))
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits.append(er(kit_id, timestamp, resource, activity_name, quantity, processed))

    p2p.save_to_database(kits)    

def cargado_en_carro_L_D():
    time.sleep(10)
    kits = p2p.get_from_database("Entrada Material Sucio", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    rack = ["Rack 1", "Rack 2", "Rack 3", "Rack 4", "Rack 5", "Rack 6", "Rack 7", "Rack 8", "Rack 9"]
    activity_name = "Cargado en Carro L+D"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed))

    p2p.save_to_database(kits_mod) 
    p2p.process(kits)  

def carga_L_D_iniciada():
    time.sleep(10)
    kits = p2p.get_from_database("Cargado en Carro L+D", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    lavadora = ["Lavadora 1", "Lavadora 2", "Lavadora 3", "Lavadora 4", "Jupiter"]
    activity_name = "Carga L+D Iniciada"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed))

    p2p.save_to_database(kits_mod) 
    p2p.process(kits)  

def carga_L_D_liberada():
    time.sleep(90)
    kits = p2p.get_from_database("Carga L+D Iniciada", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Carga L+D Liberada"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed))

    p2p.save_to_database(kits_mod) 
    p2p.process(kits)  

def montaje():
    kits = p2p.get_from_database("Carga L+D Liberada", 10)
    i = 0
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    activity_name = "Montaje"
    processed = False
    while (i < len(kits)):
        time.sleep(random.randint(0, 4))
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        if (kit_id.startswith("CONT-")):
            kits_in_container = random.randint(1, 15)
            for _ in range(kits_in_container):
                kit_id = random.choice(["EXT-", "HUBU-"]) + str(random.randint(1000, 9999))
                quantity = random.randint(1, 10)
                timestamp = datetime.now()
                montaje_kit = er(kit_id, timestamp, resource, activity_name, quantity, processed)
                p2p.save_record_to_database(montaje_kit)
                for _ in range(quantity):
                    timestamp = datetime.now()
                    quantity = 1
                    kits_mod = []
                    produccion_montada = "Produccion Montada"
                    kits_mod.append(er(kit_id, timestamp, resource, produccion_montada, quantity, processed))
                p2p.save_to_database(kits_mod) 
                p2p.process_record_using_activity_name_kit_id(montaje_kit)
        else:
            quantity = random.randint(1, 10)
            montaje_kit = kits[i]
            montaje_kit.quantity = quantity
            p2p.modify_record(montaje_kit)
            for _ in range(quantity):
                timestamp = datetime.now()
                quantity = 1
                kits_mod = []
                produccion_montada = "Produccion Montada"
                kits_mod.append(er(kit_id, timestamp, resource, produccion_montada, quantity, processed))
            p2p.save_to_database(kits_mod) 
            p2p.process_record(montaje_kit)
        i = i+1

def composicion_de_cargas():
    time.sleep(10)
    kits = p2p.get_from_database("Produccion Montada", 20)
    resources = ["VA", "MA", "SSA", "DB", "AA"]
    Esterilizador = ["Autoclave 1", "Autoclave 2", "Autoclave 3", "Autoclave 4", "Jupiter"]
    activity_name = "Composicion de Cargas"
    kits_mod = []
    for i in range(len(kits)):
        kit_id = kits[i].kit_id
        timestamp = datetime.now()
        resource = random.choice(resources)
        quantity = 1
        processed = False
        kits_mod.append(er(kit_id, timestamp, resource, activity_name, quantity, processed))

    p2p.save_to_database(kits_mod) 
    p2p.process(kits)  

def carga_de_esterilizador_liberada():
    time.sleep(90)
    kits = p2p.get_from_database("Composicion de Cargas", 20)
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

    p2p.save_to_database(kits_mod) 
    p2p.process(kits)  

def comisionado():
    time.sleep(90)
    kits = p2p.get_from_database("Carga de Esterilizador Liberada", 20)
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

    p2p.save_to_database(kits_mod) 
    p2p.process(kits)  

# Define the function for the task
def my_task_function():
    # Simulate a delay of 100 seconds
    time.sleep(10)
    print("Task completed after 100 seconds.")

with DAG(
    "croma",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["a.a.m.s.abu.sbeit@student.tue.nl"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="Croma process simulator DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=True,
    tags=["Croma"],
    #schedule_interval=None
) as dag:
    
    t1 = PythonOperator(
        task_id="entrada_material_sucio",
        python_callable=entrada_material_sucio,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="cargado_en_carro_L-D",
        python_callable=cargado_en_carro_L_D,
        provide_context=True
    )
        
    t3 = PythonOperator(
        task_id="carga_L-D_iniciada",
        python_callable=carga_L_D_iniciada,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="carga_L-D_liberada",
        python_callable=carga_L_D_liberada,
        provide_context=True
    )
        
    t5 = PythonOperator(
        task_id="montaje",
        python_callable=montaje,
        provide_context=True
    )

    t6 = PythonOperator(
        task_id="produccion_montada",
        python_callable=my_task_function,
        provide_context=True
    )
        
    t7 = PythonOperator(
        task_id="composicion_de_cargas",
        python_callable=composicion_de_cargas,
        provide_context=True
    )

    t8 = PythonOperator(
        task_id="carga_de_esterilizador_liberada",
        python_callable=carga_de_esterilizador_liberada,
        provide_context=True
    )
    
    t9 = PythonOperator(
        task_id="comisionado",
        python_callable=comisionado,
        provide_context=True
    )


    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9


