import psycopg2
from EventRecord import EventRecord as er
import pandas as pd
from Kit import Kit as kit

def create_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

def save_to_database(table, records):
    conn = create_connection()
    cur = conn.cursor()
    try:
        for record in records:
            cur.execute(f"""
                    INSERT INTO {table} (kit_id, timestamp, resource, activity_name, quantity, processed, rack, washing_machine, sterilization_machine)
                    VALUES ('{record.kit_id}', '{record.timestamp}', '{record.resource}', '{record.activity_name}', {record.quantity}, {record.processed}, '{record.rack}', '{record.washing_machine}', '{record.sterilization_machine}');
            """)
        conn.commit()
        print("Records saved to the database successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def save_record_to_database(table, record):
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
                    INSERT INTO {table} (kit_id, timestamp, resource, activity_name, quantity, processed, rack, washing_machine, sterilization_machine)
                    VALUES ('{record.kit_id}', '{record.timestamp}', '{record.resource}', '{record.activity_name}', {record.quantity}, {record.processed}, '{record.rack}', '{record.washing_machine}', '{record.sterilization_machine}');
            """)
        conn.commit()
        print("Record saved to the database successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_from_database(table, activity_name, size):
    conn = create_connection()
    records = []
    cur = conn.cursor()
    try:
        cur.execute(f"""
                    SELECT * 
                    FROM {table} 
                    WHERE activity_name = '{activity_name}' and processed = False
                    ORDER BY timestamp 
                    LIMIT {size};
            """)
        rows = cur.fetchall()
        for row in rows:
            id, kit_id, timestamp, resource, activity_name, quantity, rack, washin_machine, sterilization_machine, processed = row
            records.append(er(kit_id, timestamp, resource, activity_name, quantity, processed, id, rack, washin_machine, sterilization_machine))
    except Exception as e:
        print("Error:", e)
        conn.rollback() 
    finally:
        cur.close()
        conn.close()
    return(records)

def get_from_database_using_activity_name_kit_id(table, activity_name, kit_id):
    conn = create_connection()
    kits = []
    cur = conn.cursor()
    try:
        cur.execute(f"""
                    SELECT * 
                    FROM {table} 
                    WHERE activity_name = '{activity_name}' and kit_id = {kit_id} and processed = False
                    ORDER BY timestamp 
                    LIMIT 1;
            """)
        row = cur.fetchall()
        id, kit_id, timestamp, resource, activity_name, quantity, rack, washin_machine, sterilization_machine, processed = row
        record= er(kit_id, timestamp, resource, activity_name, quantity, processed, id, rack, washin_machine, sterilization_machine)
    except Exception as e:
        print("Error:", e)
        conn.rollback() 
    finally:
        cur.close()
        conn.close()
    return(record)

def modify(table, records):
    conn = create_connection()
    cur = conn.cursor()
    try:
        for record in records:
            cur.execute(f"""
                        UPDATE {table} 
                        SET 
                            kit_id = '{record.kit_id}',
                            timestamp = '{record.timestamp}', 
                            resource = '{record.resource}', 
                            activity_name = '{record.activity_name}', 
                            quantity = {record.quantity},
                            processed = {record.processed},
                            rack = '{record.rack}',
                            washing_machine = '{record.washing_machine}',
                            sterilization_machine = '{record.sterilization_machine}'
                        WHERE id = {record.id};
            """)
        conn.commit()
        print("Records modified in the database successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def modify_record(table, record):
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
                        UPDATE {table} 
                        SET 
                            kit_id = '{record.kit_id}',
                            timestamp = '{record.timestamp}', 
                            resource = '{record.resource}', 
                            activity_name = '{record.activity_name}', 
                            quantity = {record.quantity},
                            processed = {record.processed},
                            rack = '{record.rack}',
                            washing_machine = '{record.washing_machine}',
                            sterilization_machine = '{record.sterilization_machine}'
                        WHERE id = {record.id};
            """)
        conn.commit()
        print("Record modified in the database successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def process(table, records):
    conn = create_connection()
    cur = conn.cursor()
    try:
        for record in records:
            cur.execute(f"""
                        UPDATE {table} 
                        SET 
                            processed = True
                        WHERE id = {record.id};
            """)
        conn.commit()
        print("Records processed in the database successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def process_record(table, record):
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
                        UPDATE {table} 
                        SET 
                            processed = True
                        WHERE id = {record.id};
            """)
        conn.commit()
        print("Record processed in the database successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def process_record_using_activity_name_kit_id(table, record):
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
                        UPDATE {table} 
                        SET 
                            processed = True
                        WHERE kit_id = '{record.kit_id}' and activity_name = '{record.activity_name}';
            """)
        conn.commit()
        print("Record processed in the database successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_data_table(table_name):
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                kit_id VARCHAR(25),
                timestamp TIMESTAMP,
                resource VARCHAR(10),
                activity_name VARCHAR(50),
                quantity INT,
                rack VARCHAR(20),
                washing_machine VARCHAR(20),
                sterilization_machine VARCHAR(20),
                processed BOOL
            );
        """)
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_kits_table():
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS kits (
                id VARCHAR(25) PRIMARY KEY,
                code VARCHAR(25),
                ns int,
                count int,
                isContainedKit BOOL
            );
        """)
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_containers_table():
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS containers (
                id VARCHAR(25) PRIMARY KEY,
                code VARCHAR(25),
                ns int,
                numOfKitsContained int
            );
        """)
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def fill_kits():
    df = pd.read_csv("/Users/abdalrhman/Documents/bdma-thesis/python/data/kits.csv", delimiter=",", header=0)
    conn = create_connection()
    cur = conn.cursor()
    try:
        for index, row in df.iterrows():
            ns = int(row['ns'])
            count = int(row['count'])
            cur.execute(f"""
            Insert into kits (id, code, ns, count, isContainedKit) 
                        values ('{row['id']}', '{row['code']}', {ns}, {count}, False);
        """)
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def fill_contained_kits():
    df = pd.read_csv("/Users/abdalrhman/Documents/bdma-thesis/python/data/contained_kits.csv", delimiter=",", header=0)
    conn = create_connection()
    cur = conn.cursor()
    try:
        for index, row in df.iterrows():
            ns = int(row['ns'])
            count = int(row['count'])
            cur.execute(f"""
            Insert into kits (id, code, ns, count, isContainedKit) 
                        values ('{row['id']}', '{row['code']}', {ns}, {count}, True);
        """)
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def fill_containers():
    df = pd.read_csv("/Users/abdalrhman/Documents/bdma-thesis/python/data/containers.csv", delimiter=",", header=0)
    conn = create_connection()
    cur = conn.cursor()
    try:
        for index, row in df.iterrows():
            ns = int(row['ns'])
            numOfKitsContained = int(row['numOfKitsContained'])
            cur.execute(f"""
            Insert into containers (id, code, ns, numOfKitsContained) 
                        values ('{row['id']}', '{row['code']}', {ns}, {numOfKitsContained});
        """)
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def check_data_exists(query):
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()[0]  
    cur.close()
    conn.close()
    return result > 0

def get_kits(query):
    conn = create_connection()
    kits = []
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            id, code, ns, count, isContainedKit = row
            kits.append(kit(id, code, ns, count, isContainedKit))
    except Exception as e:
        print("Error:", e)
        conn.rollback() 
    finally:
        cur.close()
        conn.close()
    return(kits)

def get_containers(query):
    conn = create_connection()
    kits = []
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            id, code, ns, numOfKitsContained = row
            kits.append(kit(id, code, ns, numOfKitsContained= numOfKitsContained))
    except Exception as e:
        print("Error:", e)
        conn.rollback() 
    finally:
        cur.close()
        conn.close()
    return(kits)

def reset_database():
    conn = create_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""drop table kits;""")
        cur.execute(f"""drop table containers;""")
        cur.execute(f"""drop table entrada_material_sucio;""")
        cur.execute(f"""drop table cargado_en_carro_L_D;""")
        cur.execute(f"""drop table carga_L_D_iniciada;""")
        cur.execute(f"""drop table carga_L_D_liberada;""")
        cur.execute(f"""drop table montaje;""")
        cur.execute(f"""drop table producction_montada;""")
        cur.execute(f"""drop table composicion_de_cargas;""")
        cur.execute(f"""drop table carga_de_esterilizador_liberada;""")
        cur.execute(f"""drop table comisionado;""")
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()