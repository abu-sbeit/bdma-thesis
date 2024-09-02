from datetime import datetime, timedelta
import py2postgres as p2p
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def create_entrada_material_sucio_table():
    p2p.create_data_table("entrada_material_sucio")    

def create_cargado_en_carro_L_D_table():
    p2p.create_data_table("cargado_en_carro_L_D")    

def create_carga_L_D_iniciada_table():
    p2p.create_data_table("carga_L_D_iniciada")    

def create_carga_L_D_liberada_table():
    p2p.create_data_table("carga_L_D_liberada")    

def create_montaje_table():
    p2p.create_data_table("montaje")    

def create_producction_montada_table():
    p2p.create_data_table("produccion_montada")    

def create_composicion_de_cargas_table():
    p2p.create_data_table("composicion_de_cargas")    

def create_carga_de_esterilizador_liberada_table():
    p2p.create_data_table("carga_de_esterilizador_liberada")    

def create_comisionado_table():
    p2p.create_data_table("comisionado")    

def create_kits_table():
    p2p.create_kits_table()  
    
def create_containers_table():
    p2p.create_containers_table()  

def fill_kits():
    p2p.fill_kits()
    p2p.fill_contained_kits()

def fill_containers():
    p2p.fill_containers()

    
with DAG(
    "croma_framework_initializer",
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
    description="Croma framework initializer DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=True,
    tags=["Croma Framework Initializer"],
    #schedule_interval=None
) as dag:
    
    t1 = PythonOperator(
        task_id="create_entrada_material_sucio_table",
        python_callable=create_entrada_material_sucio_table,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="create_cargado_en_carro_L_D_table",
        python_callable=create_cargado_en_carro_L_D_table,
        provide_context=True
    )
        
    t3 = PythonOperator(
        task_id="create_carga_L_D_iniciada_table",
        python_callable=create_carga_L_D_iniciada_table,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="create_carga_L_D_liberada_table",
        python_callable=create_carga_L_D_liberada_table,
        provide_context=True
    )
        
    t5 = PythonOperator(
        task_id="create_montaje_table",
        python_callable=create_montaje_table,
        provide_context=True
    )

    t6 = PythonOperator(
        task_id="create_producction_montada_table",
        python_callable=create_producction_montada_table,
        provide_context=True
    )
        
    t7 = PythonOperator(
        task_id="create_composicion_de_cargas_table",
        python_callable=create_composicion_de_cargas_table,
        provide_context=True
    )

    t8 = PythonOperator(
        task_id="create_carga_de_esterilizador_liberada_table",
        python_callable=create_carga_de_esterilizador_liberada_table,
        provide_context=True
    )
    
    t9 = PythonOperator(
        task_id="create_comisionado_table",
        python_callable=create_comisionado_table,
        provide_context=True
    )

    t10 = PythonOperator(
        task_id = "create_kits_table",
        python_callable=create_kits_table,
        provide_context=True
    )

    t11 = PythonOperator(
        task_id = "create_containers_table",
        python_callable=create_containers_table,
        provide_context=True
    )

    t12 = PythonOperator(
        task_id = "fill_kits",
        python_callable=fill_kits,
        provide_context=True
    )

    t13 = PythonOperator(
        task_id = "fill_containers",
        python_callable=fill_containers,
        provide_context=True
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12 >> t13


