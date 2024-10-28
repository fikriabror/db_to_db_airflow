from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime


def extract_data():
    try:
        source_hook = PostgresHook(postgres_conn_id='source_postgre')
        with source_hook.get_conn() as source_conn:
            sql = "SELECT * FROM sheetsummaries;"
            df = pd.read_sql(sql, source_conn)
        return df
    except Exception as e:
        print(f'Error during data extraction: {e}')
        raise

def transform_data(df):
    df['fund_code'] = df['fund_code'].str.replace("'",'').astype(int)
    df['due_from_fdic_corp_and_receivables']=df['due_from_fdic_corp_and_receivables'].str.replace('NULL','0').astype(int)
    df['assets_in_liquidation']=df['assets_in_liquidation'].str.replace('-','0').astype(int)
    df['estimated_loss_on_assets_in_liquidation']=df['estimated_loss_on_assets_in_liquidation'].str.replace('NULL','0').astype(int)
    df['failure_date'] = pd.to_datetime(df['failure_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    return df

def load(df):
    try:
        target_hook = PostgresHook(postgres_conn_id='dest_postgre')
        target_conn = target_hook.get_conn()
        cursor = target_conn.cursor()
        
        # Check if the target table exists
        check_table_exists_query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'sheetsummaries'
        );
        """
        
        cursor.execute(check_table_exists_query)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS sheetsummaries (
                fund_code int,
                receivership varchar(32),
                "year" int4,
                quarter varchar(2),
                failure_date date,
                cash_and_investments int4,
                due_from_fdic_corp_and_receivables int,
                assets_in_liquidation int,
                estimated_loss_on_assets_in_liquidation int,
                total_assets int4
            );
            """
            cursor.execute(create_table_query)
        
        insert_query = """
        INSERT INTO sheetsummaries (fund_code, receivership, "year", quarter, failure_date, cash_and_investments, due_from_fdic_corp_and_receivables, \
            assets_in_liquidation, estimated_loss_on_assets_in_liquidation, total_assets)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['fund_code'],
                row['receivership'],
                row['year'],
                row['quarter'],
                row['failure_date'],
                row['cash_and_investments'],
                row['due_from_fdic_corp_and_receivables'],
                row['assets_in_liquidation'],
                row['estimated_loss_on_assets_in_liquidation'],
                row['total_assets']
            ))

        target_conn.commit()  # Commit the transaction
        
    except Exception as e:
        print(f'Error during data loading: {e}')
        raise
    finally:
        cursor.close()
        target_conn.close()

def etl_process():   
    df = extract_data()
    transform = transform_data(df)
    load(transform)

# Define the default arguments
default_args = {
    'owner': 'fikri',
    'start_date': datetime.now(),
    'retries': 1,
}

with DAG(
    dag_id='db_to_db_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define the task
    db_to_db_task = PythonOperator(
        task_id='db_to_db_task',
        python_callable=etl_process,
    )

# Set the task sequence
db_to_db_task
