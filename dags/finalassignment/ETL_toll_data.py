from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import pandas as pd

finalassignment = '/home/wahba/PycharmProjects/newairflow/dags/finalassignment'
staging = f'{finalassignment}/staging'
tolldata=f'{finalassignment}/tolldata'


def extractcsv():
    columns = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Vehicle code']
    df_csv = pd.read_csv(f'{tolldata}/vehicle-data.csv', names=columns)
    extracted_df = df_csv[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]
    extracted_df.to_csv(f'{staging}/csv_data.csv', index=False)
    print('extracted csv successfully')


def extractTSV():
    columns = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id',
               'Tollplaza code']
    tsv = pd.read_csv(f'{tolldata}/tollplaza-data.tsv', sep='\t', names=columns)
    extracred_df = tsv[['Number of axles', 'Tollplaza id', 'Tollplaza code']]
    extracred_df.to_csv(f'{staging}/tsv_data.csv', index=False)
    print('extracted tsv successfully')


def extractfwf():
    columns = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Tollplaza id', 'Tollplaza code',
               'Type of Payment code', 'Vehicle Code']
    widths = [7, 25, 11, 5, 10, 4, 5]
    fwf = pd.read_fwf(f'{tolldata}/payment-data.txt', widths=widths, names=columns)
    extracted_fwf = fwf[['Type of Payment code', 'Vehicle Code']]
    extracted_fwf.to_csv(f'{staging}/fixed_width_data.csv', index=False)
    print('extracted fwf successfully')


def merging_files():
    csv = pd.read_csv(f'{staging}/csv_data.csv')
    tsv = pd.read_csv(f'{staging}/tsv_data.csv')
    fwf = pd.read_csv(f'{staging}/fixed_width_data.csv')
    merged_data = pd.concat([csv, tsv, fwf], axis=1)
    merged_data.to_csv(f'{staging}/extracted_data.csv', index=False)
    print('finished extracting successfully')


def transforming_data():
    extracted_data = pd.read_csv(f'{staging}/extracted_data.csv')
    extracted_data['Vehicle type'] = extracted_data['Vehicle type'].str.upper()
    transformed_data = extracted_data
    transformed_data.to_csv(f'{staging}/transformed_data.csv', index=False)


default_args = {
    'owner': 'wahba',
    'start_date': days_ago(0),
    'email': 'bedoa625@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# defining the dag
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

# define the tasks
# define the first task unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzf {finalassignment}/tolldata.tgz -C {tolldata}',
    dag=dag
)

# define second task extract_data_from_csv
extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extractcsv,
    dag=dag
)

# define third task extract_data_from_tsv
extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extractTSV,
    dag=dag
)

# define fourth task extract_data_from_fixed_width
extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extractfwf,
    dag=dag
)
# define fifth task consolidate_data
consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=merging_files,
    dag=dag
)

# define sixth task transform_data
transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transforming_data,
    dag=dag
)
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
