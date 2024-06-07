import boto3
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import pyarrow
import os

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION')


def extractor_from_raw_to_bronze():
    print("Initializing data extraction from Raw Folder")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

    BUCKET_NAME = "dnc-lucas-tutorial-class"
    FILE_KEY = "ecommerce/raw/orders.csv"

    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)

    df = pd.read_csv(response["Body"])

    print("Dataframe Orders", df.info())

    print("Saving Data into Bronze Layer")

    pq.write_to_dataset(
        pyarrow.Table.from_pandas(df),
        'dnc-lucas-tutorial-class/ecommerce/bronze',
        filesystem=s3fs.S3FileSystem()
    )
