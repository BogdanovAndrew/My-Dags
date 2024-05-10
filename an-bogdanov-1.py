import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df["zone"] = top_data_df.domain.str.split(".", expand=True)[1]
    top_data_top_10 = top_data_df.groupby("zone", as_index=False).agg({"rank" : "count"}).sort_values("rank", ascending=False)
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))

def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    pos_airflow = top_data_df[top_data_df['domain'] == "airflow.com"]
    with open('pos_airflow.csv', 'w') as f:
        f.write(pos_airflow["rank"].to_csv(index=False, header=False))
        
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df["domain_length"] = top_data_df.domain.str.len()
    longest_domains = top_data_df[top_data_df['domain'].str.len() == top_data_df.domain_length.max()].head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domains["domain"].to_csv(index=False, header=False))

def print_data(ds):
    date = ds
    with open('top_data_top_10.csv', 'r') as f:
        top_10 = f.read()
    
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    
    with open('pos_airflow.csv', 'r') as f:
        pos_airflow = f.read()
        
    print(f'Top-10 domains zone for date {date}:')
    print(top_10)
    
    print(f'The longest_domain for date {date}:')
    print(longest_domain)
    
    print(f'Airflow.com position for date {date}:')
    print(pos_airflow)


default_args = {
    'owner': 'an-bogdanov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
    'start_date': datetime(2024, 3, 8),
    'schedule_interval' : '*/60 * * * *'
}

dag = DAG('an-bogdanov-homework-task-1', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2 = PythonOperator(task_id='top_data_top_10',
                    python_callable=get_top_10,
                    dag=dag)
t3 = PythonOperator(task_id='longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)
t4 = PythonOperator(task_id='pos_airflow',
                        python_callable=get_airflow,
                        dag=dag)
t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
