import pandas as pd
from minio import Minio
from sqlalchemy import create_engine

def load_into_psql():
    
    client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    response = client.get_object("raw-data", "events/events.csv")
    df = pd.read_csv(response)

    engine = create_engine("postgresql://dbt:dbt@localhost:5432/analytics")

    df.to_sql("events_raw", engine, schema="public", if_exists="replace", index=False)

    print("All data loaded in Postgres (event raw)")




if __name__ == "__main__":
    print("Start Load to Psg from minio")
    load_into_psql()