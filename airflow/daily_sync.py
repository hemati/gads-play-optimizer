from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd

from app.openai_client import get_recommendations


def export_ads():
    return pd.DataFrame()


def export_play():
    return pd.DataFrame()


def build_compact_json(df_ads: pd.DataFrame, df_play: pd.DataFrame) -> dict:
    return {}


def store_results(reco: dict):
    print(reco)


def pipeline(**_):
    df_ads = export_ads()
    df_play = export_play()
    compact = build_compact_json(df_ads, df_play)
    recommendations = get_recommendations(compact)
    store_results(recommendations.dict())


default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}
with DAG(
    dag_id="daily_gads_play_sync",
    schedule_interval="0 8 * * *",
    start_date=datetime(2025, 6, 19),
    catchup=False,
    default_args=default_args,
) as dag:
    run_pipeline = PythonOperator(task_id="run_pipeline", python_callable=pipeline)
