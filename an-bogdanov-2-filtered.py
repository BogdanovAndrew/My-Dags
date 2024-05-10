import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'an-bogdanov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 8),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def an_bogdanov_homework_task_2_filtered():
    @task(retries=3)
    def get_data():
        df = pd.read_csv("/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv")
        login = "an-bogdanov"
        #year = 1994 + hash(f'{login}') % 23
        year = 2015
        df = df[df.Year == year]
        return df.to_csv(index=False)

    @task(retries=4, retry_delay=timedelta(10))
    def top_game(df):
        top_game_df = pd.read_csv(StringIO(df))
        top_game = top_game_df.sort_values("Global_Sales", ascending=False).head(1)["Name"].values[0]
        return top_game

    @task()
    def top_genre_eu(df):
        top_genre_eu_df = pd.read_csv(StringIO(df))
        top_genre_eu = top_genre_eu_df.groupby("Genre", as_index=False).agg({"EU_Sales" : "sum"}).sort_values("EU_Sales", ascending=False).head(1)["Genre"].values[0]
        return top_genre_eu

    @task()
    def top_platform_na(df):
        top_platform_na_df = pd.read_csv(StringIO(df))
        top_platform_na_df["NA_Sales > 1"] = top_platform_na_df["NA_Sales"] > 1
        top_platform_na = top_platform_na_df.groupby("Platform", as_index=False).agg({"NA_Sales > 1" : "sum"}).sort_values("NA_Sales > 1", ascending=False).head(1)["Platform"].values[0]
        return top_platform_na

    @task()
    def top_publisher_jp(df):
        top_publisher_jp_df = pd.read_csv(StringIO(df))
        top_publisher_jp = top_publisher_jp_df.groupby("Publisher", as_index=False).agg({"JP_Sales" : "mean"}).sort_values("JP_Sales", ascending=False).head(1)["Publisher"].values[0]
        return top_publisher_jp
    
    @task()
    def count_games_eu_jp(df):
        count_games_eu_jp_df = pd.read_csv(StringIO(df))
        count_games_eu_jp_df["eu > jp"] = count_games_eu_jp_df["EU_Sales"] > count_games_eu_jp_df["JP_Sales"]
        count_games_eu_jp = count_games_eu_jp_df["eu > jp"].sum()
        return count_games_eu_jp
        
    @task()
    def print_data(top_game, top_genre_eu, top_platform_na, top_publisher_jp, count_games_eu_jp):

        context = get_current_context()
        date = context['ds']

        #my_Year = 1994 + hash("an-bogdanov") % 23
        my_Year = 2015

        print(f'''Дата выполнения {date}
                  Отчет за {my_Year} год
                  Самая продаваемая игра во всем мире: {top_game}
                  Самый продаваемый жанр в Европе: {top_genre_eu}
                  Платформы, на которых было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке: {top_platform_na}
                  Издатели, у которых были самые высокие средние продажи в Японии: {top_publisher_jp}
                  Количество игр, которые продались лучше в Европе, чем в Японии: {count_games_eu_jp}''')

    df = get_data()
    
    top_game = top_game(df)
    top_genre_eu = top_genre_eu(df)
    top_platform_na = top_platform_na(df)
    top_publisher_jp = top_publisher_jp(df)
    count_games_eu_jp = count_games_eu_jp(df)

    print_data(top_game,
               top_genre_eu,
               top_platform_na,
               top_publisher_jp,
               count_games_eu_jp)

an_bogdanov_homework_task_2_filtered = an_bogdanov_homework_task_2_filtered()
