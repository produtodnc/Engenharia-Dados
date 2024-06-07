import boto3
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import pyarrow
import os
from io import BytesIO

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION')


def transformer_process_from_bronze_to_silver():
    print("Initiazling data transformation from Bronze to Silver")

    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=aws_region)
    bucket_name = 'dnc-lucas-tutorial-class'
    directory_path = 'ecommerce/bronze/'

    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=directory_path
    )

    dfs = []

    for obj in response.get('Contents', []):
        file_key = obj["Key"]

        if ".parquet" in file_key:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            parquet_file = BytesIO(response["Body"].read())

            df = pq.read_table(parquet_file).to_pandas()

            dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    print("combined_df", combined_df.info())

    print("Remvoving Null Values")
    df_droped = combined_df.dropna(axis=0)

    print("Filtering only Finished Orders")
    df_droped_delivered = df_droped[df_droped["order_status"] == "delivered"]

    print("Saving Data Into Silver Layer")
    pq.write_to_dataset(
        pyarrow.Table.from_pandas(df_droped_delivered),
        'dnc-lucas-tutorial-class/ecommerce/silver/fact_table',
        filesystem=s3fs.S3FileSystem()
    )


def orders_to_logistic():
    print("Initializing transformation to logistic")

    print("Loading Fact Table Data")
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=aws_region)
    bucket_name = 'dnc-lucas-tutorial-class'
    directory_path = 'ecommerce/silver/fact_table'

    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=directory_path)

    dfs = []

    # Loop through each Parquet file in the directory
    for obj in response.get('Contents', []):
        # Get the file key (path) for each Parquet file
        file_key = obj['Key']

        if ".parquet" in file_key:
            # Load the Parquet file from S3 into memory
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            parquet_file = BytesIO(response['Body'].read())

            # Read the Parquet file using pyarrow and convert it to a Pandas DataFrame
            df = pq.read_table(parquet_file).to_pandas()

            # Append the DataFrame to the list
            dfs.append(df)

    fact_table_df = pd.concat(dfs, ignore_index=True)

    fact_table_df["key"] = fact_table_df["customer_id"].astype(str)

    print("Fact Table Info", fact_table_df.info())

    print("Loading Customer's Dimension")
    file_key = "ecommerce/silver/customers/cf98a375ca4546a49616d96a6969971f-0.parquet"
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_key
    )

    parquet_file = BytesIO(response['Body'].read())

    df_customers = pq.read_table(parquet_file).to_pandas()
    print("df_customers", df_customers.info())

    df_customers["key"] = df_customers["customer_id"].astype(str)

    print("Customer Info", df_customers.info())

    df_merged = pd.merge(
        fact_table_df,
        df_customers,
        on="key",
        how="inner"
    )

    df_merged = df_merged[["customer_state", "customer_city", "order_id"]]

    df_merged["state_city"] = df_merged["customer_state"].str.cat(
        df_merged["customer_city"], sep="-")

    df_merged_grouped = df_merged[["state_city", "order_id"]].groupby(
        "state_city").count().reset_index()

    print("Logistic Dataframe Sample", df_merged_grouped.sample(10))

    print("Saving Data into Golden Layer")

    pq.write_to_dataset(
        pyarrow.Table.from_pandas(df_merged_grouped),
        'dnc-lucas-tutorial-class/ecommerce/gold/logistic',
        filesystem=s3fs.S3FileSystem()
    )


def orders_to_finance():
    print("Initializing transformation to finance")

    print("Loading Fact Table Data")
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=aws_region)
    bucket_name = 'dnc-lucas-tutorial-class'
    directory_path = 'ecommerce/silver/fact_table'

    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=directory_path)

    dfs = []

    # Loop through each Parquet file in the directory
    for obj in response.get('Contents', []):
        # Get the file key (path) for each Parquet file
        file_key = obj['Key']

        if ".parquet" in file_key:
            # Load the Parquet file from S3 into memory
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            parquet_file = BytesIO(response['Body'].read())

            # Read the Parquet file using pyarrow and convert it to a Pandas DataFrame
            df = pq.read_table(parquet_file).to_pandas()

            # Append the DataFrame to the list
            dfs.append(df)

    fact_table_df = pd.concat(dfs, ignore_index=True)

    fact_table_df["key"] = fact_table_df["order_id"].astype(str)

    print("Fact Table Data", fact_table_df.info())

    print("Loading Sales's Data")
    file_key = 'ecommerce/silver/orders_details/197b41a151a046febd4e11adc80c4a96-0.parquet'
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    parquet_file = BytesIO(response['Body'].read())

    # Read the Parquet file using pyarrow and convert it to a Pandas DataFrame
    df_sales = pq.read_table(parquet_file).to_pandas()
    df_sales["key"] = df_sales["order_id"].astype(str)
    print("Customers Data", df_sales.info())

    df_merged = pd.merge(fact_table_df, df_sales, on="key", how="inner")
    print("Data Frame Merged", df_merged.info())

    df_merged = df_merged[["payment_value", "customer_id"]]
    df_merged_grouped = df_merged.groupby(
        "customer_id")["payment_value"].sum().reset_index()

    print("Sample", df_merged_grouped.head(5))

    print("Saving Data into Golden Layer Finance")
    pq.write_to_dataset(pyarrow.Table.from_pandas(df_merged_grouped), 'dnc-lucas-tutorial-class/ecommerce/gold/finance',
                        filesystem=s3fs.S3FileSystem())
