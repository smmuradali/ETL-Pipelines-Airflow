# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'M Ali',
    'start_date': days_ago(0),
    'email': ['mali@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the task 'unzip_data'

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -xvzf /home/project/tolldata.tgz',
    dag=dag,
)
# define the task 'extract_data_from_csv'

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='sudo cut -f1-4 -d"," /home/project/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)

# define the task 'extract_data_from_tsv'

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='sudo cut -f5,6,7 /home/project/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)


# define the task 'extract_data_from_fixed_width'

#extract = BashOperator(
 #   task_id='part1',
 #   bash_command='sudo tr -s '[:space:]' < /home/project/payment-data.txt > part1.csv',
 #   dag=dag,
#)


# define the task 'extract_data_from_fixed_width'

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    #bash_command='sudo cut -d " " -f 10-11 part1.csv >  /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    bash_command= 'sudo cut -f5-7 /home/project/tollplaza-data.tsv  | tr "\t" "," > /home/project/airflow/dags/fixed_width_data.csv',
    dag=dag,
)

# define the task 'consolidate_data'

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='sudo paste /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv >  /home/project/airflow/dags/finalassignment/staging/extracted_data.csv ',
    dag=dag,
)

# define the task 'transform'

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignment/staging/extracted_data.csv> /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)



# task pipeline

unzip_data >> extract_data_from_csv >>  extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
