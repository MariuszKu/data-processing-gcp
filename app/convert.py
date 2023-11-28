import polars as pl
import datetime
import gcsfs
import argparse
import pandas as pd
import os

fs = gcsfs.GCSFileSystem()
LANDING = os.environ["LANDING"]
SILVER = os.environ["SILVER"]

def convert_to_parquet_clients():
    
    df = pl.read_csv(f"gs://{LANDING}/clients.csv")
    df.with_columns( pl.col("name").str.to_uppercase().alias("name"))
    df.with_columns( pl.lit(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")).alias("load_date"))
    dest = f"gs://{SILVER}/clients/clients.parquet"
    df = df.drop("")
    print(df.schema)
    with fs.open(dest,"wb") as file:
        df.write_parquet(
            file,
            compression="snappy",
            use_pyarrow=True,
            )


def convert_to_parquet_client_applications():

    df = pl.read_csv(f"gs://{LANDING}/client_applications.csv")
    df.with_columns(pl.col("client_name").str.to_uppercase().alias("client_name"))
    df.with_columns( pl.lit(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")).alias("load_date"))
    df = df.drop("")
    dest = f"gs://{SILVER}/client_applications/client_applications.parquet"
    with fs.open(dest,"wb") as file:
        df.write_parquet(
            file,
            compression="snappy",
            use_pyarrow=True,
            )


def convert_to_parquet_card_operations():
    df = pl.read_csv(f"gs://{LANDING}/card_operations.csv")
    df.with_columns( pl.lit(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")).alias("load_date"))
    df = df.drop("")
    dest = f"gs://{SILVER}/card_operations/card_operations.parquet"
    with fs.open(dest,"wb") as file:
        df.write_parquet(
            file,
            compression="snappy",
            use_pyarrow=True,
            )
        
def convert_branch_to_parquet():
    df = pd.read_excel(f"gs://{LANDING}/branch.xlsx")
    df["load_date"] = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    #df = df.drop("")
    dest = f"gs://{SILVER}/branch/branch.parquet"
    df.to_parquet(
            dest,
            compression="snappy",
            )




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Description of your script.')
    parser.add_argument('-f', '--file', type=str)
    args = parser.parse_args()
    input_file = args.file

    if input_file == "clients":
        convert_to_parquet_clients()
    elif input_file == "applications":
        convert_to_parquet_client_applications()
    elif input_file == "transactions":
        convert_to_parquet_card_operations()
    elif input_file == "branch":
        convert_branch_to_parquet()


   
