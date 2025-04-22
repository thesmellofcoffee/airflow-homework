from datetime import datetime, timedelta

import psycopg2
import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


client = MongoClient(
    "Hocam bu kısmı github'a public yazdığım için siliyorum",
    server_api=ServerApi("1")
)
db = client["bigdata_training"]

COLL_LOGS      = "log_OrkunEnesYuksel"
COLL_USER      = "user_coll_OrkunEnesYuksel"
COLL_SAMPLE    = COLL_USER + "_sample"
COLL_ANOMALIES = COLL_USER + "_anomalies"

PG_CONN = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "airflow",
    "user":     "airflow",
    "password": "airflow",
}

default_args = {
    "owner":      "you",
    "start_date": datetime(2025, 1, 1), 
    "retries":    1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="homework_mongo_flow",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["bigdata_training"],
) as dag:

    start = DummyOperator(task_id="start")

    def insert_airflow_logs_to_mongo():
        conn = psycopg2.connect(**PG_CONN)
        sql = """
            SELECT event AS event_name, COUNT(*) AS record_count
            FROM log
            WHERE dttm >= NOW() - INTERVAL '1 minute'
            GROUP BY event
        """
        df = pd.read_sql(sql, conn)
        conn.close()

        docs = [
            {
                "event_name":   row["event_name"],
                "record_count": int(row["record_count"]),
                "created_at":   datetime.utcnow(),
            }
            for _, row in df.iterrows()
        ]
        if docs:
            db[COLL_LOGS].insert_many(docs)

    t1 = PythonOperator(
        task_id="insert_airflow_logs_into_mongodb",
        python_callable=insert_airflow_logs_to_mongo,
    )

    def create_sample_data():
        df = pd.DataFrame({
            "ts":    pd.date_range(datetime.utcnow() - timedelta(hours=1), periods=100, freq="T"),
            "value": pd.np.random.randn(100) * 10 + 50,
        })
        records = df.to_dict(orient="records")
        db[COLL_SAMPLE].insert_many(records)

    t2 = PythonOperator(
        task_id="create_sample_data",
        python_callable=create_sample_data,
    )

    def copy_anomalies():
        df   = pd.DataFrame(list(db[COLL_SAMPLE].find({})))
        mean = df["value"].mean()
        std  = df["value"].std()

        mask       = (df["value"] > mean + 3*std) | (df["value"] < mean - 3*std)
        anomalies  = df.loc[mask].to_dict(orient="records")
        for doc in anomalies:
            doc.pop("_id", None)

        if anomalies:
            db[COLL_ANOMALIES].insert_many(anomalies)

    t3 = PythonOperator(
        task_id="copy_anomalies_into_new_collection",
        python_callable=copy_anomalies,
    )

    final = DummyOperator(task_id="finaltask")

    start >> [t1, t2]
    t1    >> final
    t2    >> t3 >> final
