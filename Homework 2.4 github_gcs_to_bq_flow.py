# To transfer the data from google cloud storage to big query

from pathlib import Path
import pandas as pd
import os

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.filesystems import GitHub
import argparse


@task(log_prints=True)
def extract_from_gcs(color: str, year: str, month: str) -> Path:
    """Downlaod trip data from GCS"""
    curr_path = os.getcwd()
    dataset_file = f'{color}_tripdata_{year}-{month:02}.parquet'
    gcs_path = Path(curr_path, "data", dataset_file)
    gcs_block = GcsBucket.load("taxi-gcs")
    # I have the local and gcs path as the same one in this case
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    return gcs_path


@task(log_prints=True)
def write_bq(df: pd.DataFrame, color) -> None:
    """Write DataFrame to BigQuery"""

    # pandas have a bigquery function ... nice
    gcp_credentials_block = GcpCredentials.load("gcp-taxi-cred")
    df.to_gbq(destination_table=f"taxi_dataset.{color}_taxi_rides",
              project_id="striped-harbor-375816",
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              chunksize=500_000,
              if_exists="append")


@flow(log_prints=True)
def etl_gcs_to_bq(month: int, year: int, color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    print("GCS PATH")
    print(path)
    df = transform(path)
    write_bq(df, color)
    return len(df)

@flow(log_prints=True)
def etl_gcs_to_bq_parent(months_list: list, year: int, color: str):
    row_sum = 0
    for month in months_list:
        processed_rows = etl_gcs_to_bq(month, year, color)
        row_sum += processed_rows
    print("Processed Rows: " + str(row_sum))


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # Not needed for homework
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df
    
    
if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description='parameters for identifying the correct taxi data')
#     parser.add_argument('--year', required=True, help='year of the taxi ride data')
#     parser.add_argument('--month', required=True, help='month of the taxi ride data')
#     parser.add_argument('--color', required=True, help='color of the taxi')
#     args = parser.parse_args()
#     monthlist = []
#     monthlist.append(args.month)
    etl_gcs_to_bq_parent([11], 2020, "green")
