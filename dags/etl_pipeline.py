from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
from scraping_data import fetch_data, process_matches, process_standings, generate_insert_sql
import pandas as pd
import sys, os 

sys.path.append(os.path.abspath("./plugins"))

# Seasons to process
seasons = ["2023/2024", "2022/2023", "2021/2022", "2020/2021", "2019/2020", "2018/2019", "2017/2018",
           "2016/2017", "2015/2016", "2014/2015", "2013/2014"]

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
 
def fetch_and_process_data(**kwargs):
    all_matches_data = []
    all_standings_data = []
    for season in seasons:
        matches, standings = fetch_data(season)
        all_matches_data.extend(process_matches(matches, season))
        all_standings_data.extend(process_standings(standings, season))
    
    # Push processed data to XComs
    kwargs['ti'].xcom_push(key='matches_data', value=all_matches_data)
    kwargs['ti'].xcom_push(key='standings_data', value=all_standings_data)

with DAG(
    dag_id='fotmob_dag',
    default_args=default_args,
    schedule_interval='@once',
    tags=['seria-a'],
    template_searchpath=['./plugins']
) as dag:

    # Task to fetch and process data
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_and_process_data,
        provide_context=True
    )

    # Task to create tables in the database
    create_db_table_task = PostgresOperator(
        task_id='create_db_tables',
        postgres_conn_id='FotMob_conn',
        sql='create_tables.sql'
    )


    # Task to insert matches data into the database
    def insert_matches_data(**kwargs):
        matches_data = kwargs['ti'].xcom_pull(task_ids='fetch_data', key='matches_data')
        matches_df = pd.DataFrame(matches_data)
        matches_insert_sql, matches_values = generate_insert_sql('matches', matches_df.to_dict(orient='records'), matches_df.columns)
        # Establish a new connection for this task
        hook = PostgresHook(postgres_conn_id='FotMob_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(matches_insert_sql, matches_values)
        conn.commit()
        cursor.close()
        conn.close()
        return "Matches data inserted successfully"
            

    insert_matches_data_task = PythonOperator(
        task_id='insert_matches_data',
        python_callable=insert_matches_data,
    )

    # Task to insert teams data into the database
    def insert_teams_data(**kwargs):
        teams_data = kwargs['ti'].xcom_pull(task_ids='fetch_data', key='standings_data')
        teams_df = pd.DataFrame(teams_data)
        teams_df = teams_df[['team_id', 'team_name', 'short_name']].drop_duplicates(subset=['team_id'], keep='first')
        print(teams_df)
        teams_insert_sql, teams_values = generate_insert_sql('teams', teams_df.to_dict(orient='records'), teams_df.columns)
        # Establish a new connection for this task
        hook = PostgresHook(postgres_conn_id='FotMob_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(teams_insert_sql, teams_values)
        conn.commit()
        cursor.close()
        conn.close()
        return "Teams data inserted successfully"

    insert_teams_data_task = PythonOperator(
        task_id='insert_teams_data',
        python_callable=insert_teams_data,
    )


    # Task to insert standings data into the database
    def insert_standings_data(**kwargs):
        standings_data = kwargs['ti'].xcom_pull(task_ids='fetch_data', key='standings_data')
        standings_df = pd.DataFrame(standings_data)
        standings_df = standings_df[['season','team_id','rank', 'played','wins','draws', 'losses', 'pts', 'scoreStr', 'goalConDiff']]
        standings_insert_sql, standings_values = generate_insert_sql('standings', standings_df.to_dict(orient='records'), standings_df.columns)
        # Establish a new connection for this task
        hook = PostgresHook(postgres_conn_id='FotMob_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(standings_insert_sql, standings_values)
        conn.commit()
        cursor.close()
        conn.close()
        return "Standings data inserted successfully"

    insert_standings_data_task = PythonOperator(
        task_id='insert_standings_data',
        python_callable=insert_standings_data,
    )



    # Define task dependencies
    fetch_data_task >> create_db_table_task >> [insert_matches_data_task, insert_teams_data_task ] >> insert_standings_data_task