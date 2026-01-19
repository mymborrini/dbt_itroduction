import pandas as pd
from minio import Minio
from io import BytesIO
from datetime import datetime

def generate_basic_sample():

    df = pd.DataFrame({
        "event_id": range(1,6),
        "event_type": ["click", "view", "click", "purchase", "view"],
        "created_at": [datetime.utcnow()] * 5
    })

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

    bucket = "raw-data"

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.put_object(
        bucket_name=bucket,
        object_name="events/events.csv",
        data = BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv"
    )

    print("Csv basic loaded on Minio")





if __name__ == "__main__":
    print("Start generate CSVs")
    generate_basic_sample()