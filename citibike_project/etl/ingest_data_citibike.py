#!/usr/bin/env python
# coding: utf-8

## Import necessary libraries
import os
import subprocess
import logging
import zipfile
import tarfile
from urllib.parse import urljoin
from pathlib import Path
from time import time
import argparse


import requests
import pandas as pd
from bs4 import BeautifulSoup

import sqlalchemy
from sqlalchemy import create_engine, text

from google.cloud import storage, bigquery
#from azure.storage.blob import BlobServiceClient

# Local imports
#from safe_run import safe_run



## Declare global variables
local_data_dir = Path(__file__).parent.parent / "data" / "citibike_data"
BASE_URL = "https://s3.amazonaws.com/tripdata/"

DOWNLOAD_DIR = local_data_dir

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    ## Set logging and configs
    # Set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s')

    # Connect to postgres database inside containter
    engine = create_engine(f"postgresql://{user}:{password}@localhost:{port}/{db}")
    db_connection = engine.connect()



    ## Define functions

    def scrape_citibike_files():
        """Scrape xml page from citibike aws listing"""
        xml_index_url = BASE_URL
        response = requests.get(xml_index_url)
        links = list()
        soup = BeautifulSoup(response.text, features="xml")
        xml_keys = soup.find_all('Key')

        # Extract all download links
        files = [urljoin(BASE_URL, str(link.contents[0])) for link in xml_keys if str(link.contents[0]).endswith('.zip')]
        return files


    def download_files(url, download_dir=DOWNLOAD_DIR):
        """Download and unzip files to defined directories"""            
        filecount = 0
        # create directories to store unzipped and archived files
        logging.info("Starting download: %s", url)
        try:
            os.makedirs(download_dir, exist_ok=True)
            archive_dir = Path(f"{download_dir}/archive_files") 
            file_path = Path(archive_dir) / os.path.basename(url)
            files_dir = str(os.path.basename(url)).strip('JC-citibike-tripdata.zip.csv')
            unzip_dir = Path(f"{download_dir}/unzipped_files/{files_dir}")

            # Download using subprocess and wget
            #print(f"Downloading {url} to {file_path}")
            subprocess.run(["wget", "-q", "-N", "-P", archive_dir, url], check=True)

            logging.info("Download complete: %s", file_path)

        except Exception as e:
            logging.error("Download failed: %s", e)
            raise 

        # Extract depending on file type
        if file_path.suffix == ".zip":
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(unzip_dir)
                #print(f"Extracted ZIP to {unzip_dir}")
                logging.info("zip file extraction complete: %s", file_path)
            except zipfile.BadZipFile:
                logging.error("Invalid zip file: %s", file_path)
                raise
        elif file_path.suffix in [".tar", ".gz", ".bz2"]:
            try:
                with tarfile.open(file_path, 'r:*') as tar_ref:
                    tar_ref.extractall(unzip_dir)
                #print(f"Extracted TAR to {unzip_dir}")
                logging.info("tar-like file extraction complete: %s", file_path)
            except tarfile.TarError:
                logging.error("Invalid tar file: %s", file_path)
                raise
        else:
            #print("No extraction performed, unknown file type")
            logging.warning("Unknown file type, skipping extraction: %s", file_path)
        
        filecount += 1
        print(f"Download and extraction of {os.path.basename(url)} complete. Total files processed: {filecount}")


    def find_csv_file(DOWNLOAD_DIR=DOWNLOAD_DIR):
        """search csv and parquet files in data directory"""
        unzip_dir_list = os.listdir(f"{DOWNLOAD_DIR}/unzipped_files")
        #print(unzip_dir_list)
        paths_list = []

        for dir in unzip_dir_list:
            #print(dir)
            folder = Path(f"{DOWNLOAD_DIR}/unzipped_files/{dir}")
            for file in os.listdir(folder):
                #print(str(file))
                filename = str(file)
                if filename.endswith(".csv"):
                    path = Path.joinpath(folder, filename)
                    #print(path)
                elif filename.endswith(".parquet"): #TODO: handle parquet files later
                    continue
                else:
                    continue
                paths_list.append(path)
                #print(paths_list)
        return paths_list


    def load_data_to_postgres(paths_list=find_csv_file(), 
                              engine=engine,
                              chunksize=200000):
        """Create schema in psql database and load data"""
        for path in paths_list[-1:]: # TODO: Clear the list [-1:] to ingest all files in production or in cloud
            df_name = "_".join(["citibike", str(path).split("/")[-2].strip()])
            # Create an iterator from the large dataset
            df_header = pd.read_csv(filepath_or_buffer=path,
                                parse_dates=["started_at", "ended_at"]).head(n=0)
                
            try:
                # Load the header of the df as schemas
                df_header.to_sql(name=df_name, con=engine, if_exists="replace")
                # Create an iterator from the large dataset
                df_iter = pd.read_csv(filepath_or_buffer=path,
                                    chunksize=chunksize, 
                                    parse_dates=["started_at", "ended_at"])
                while True:
                    try:
                        start_time = time()
                        chunk_num = 0
                        df = next(df_iter)
                        df.to_sql(name=df_name, con=engine, if_exists="append")
                        chunk_num += 1
                        end_time = time()
                    except StopIteration:
                        logging.info(f"Finished ingesting chunck {chunk_num} into postgres; just check if last")
                        break
                #print(f'Insertion of {df_name} complete, I/O osp time {(end_time-start_time):.2f}')
                logging.info(f"Insertion into postgres db complete: %s", df_name)
            except Exception as e:
                logging.error("Data insertion failed: %s", e)
                raise 


    def ingest_from_bigquery_to_postgres(params=params, chunk_size=500_000):
        """Ingest data from Big Query to Postgres in chunks"""

        # Replace these with your PostgreSQL credentials
        DB_USER = params.user
        DB_PASS = params.password
        DB_HOST = params.host
        DB_PORT = params.port
        DB_NAME = 'citibikebq'  # The database you want to create

        # Use these for a dictionary input for nb testing
        #DB_USER = params['user']
        #DB_PASS = params['password']
        #DB_HOST = params['host']
        #DB_PORT = params['port']
        #DB_NAME = 'citibikebq'  # The database you want to create

        # Connect to default database and create new database if not exists
        default_engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres', 
                            isolation_level='AUTOCOMMIT')
        try:
            # Execute CREATE DATABASE
            with default_engine.connect() as default_conn:
                # Check if the database exists
                result = default_conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :dbname"), {"dbname": DB_NAME})
                exists = result.scalar()  # Returns None if no rows found
                
                if not exists:
                    default_conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
                    #print(f"Database '{DB_NAME}' created successfully!")
                    logging.info(f"Database '{DB_NAME}' created successfully!")
                else:
                    #print(f"Database '{DB_NAME}' already exists.")
                    logging.info(f"Database '{DB_NAME}' already exists.")

        except Exception as e:
                    logging.error("Data insertion from Big Query failed: %s", e)
                    raise 
        
        # Connect to the newly created database and ingest data into postgres container
        citibikebq_engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}', 
                        isolation_level='AUTOCOMMIT')

        try:
            with citibikebq_engine.connect() as citibikebq_conn:
                # Initialize BigQuery client
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/bonaventure/gcp-keys.json"

                client = bigquery.Client()

                # Download data from BigQuery and load to Postgres
                for year in [2013]:#range(2013, 2015):  # TODO: Extend range until year 2019 in production or cloud environment
                    #print(year)
                    # Query BigQuery in 1 million row chunks
                    chunk_size = chunk_size  # 1 million rows per chunk
                    offset = 0

                    # Check if table exists
                    check_num = 0
                    if check_num >= 20:  # Safety check to avoid infinite loops during testing
                        logging.warning("Reached maximum number of checks, stopping to avoid infinite loop.")
                        break
                    check_table_query = f"SELECT to_regclass('public.citibike_trips_{year}')"
                    check_table_result = citibikebq_conn.execute(text(check_table_query))
                    table_exists = check_table_result.scalar()
                    

                    while True:
                        try:
                            # Skip table if exists
                            if table_exists:
                                print(f"Table 'citibike_trips_{year}' already exists in {DB_NAME}")
                                check_num += 1
                                break
                            
                            # Query BigQuery in chunks
                            query = f"""
                            SELECT *
                            FROM `bigquery-public-data.new_york_citibike.citibike_trips`
                            WHERE EXTRACT(YEAR FROM starttime) = {year}
                            LIMIT {chunk_size} OFFSET {offset}
                            """
                            
                            df_chunk = client.query(query).to_dataframe()
                            if df_chunk.empty:
                                logging.info(f"Insertion into postgres db '{DB_NAME}' complete: %s", "citibike_trips_{year}")
                                break  # stop when there is no more data
                            
                            df_chunk.to_sql(f'citibike_trips_{year}', 
                                            citibikebq_engine, 
                                            if_exists='replace', 
                                            index=False)
                            logging.info(f"Loaded rows {offset} to {offset + len(df_chunk)} into Postgres")
                            #print(f"Loaded rows {offset} to {offset + len(df_chunk)} into Postgres")

                            offset += chunk_size
                        except StopIteration:
                            logging.warning(f"Insertion into postgres db {DB_NAME} complete: citibike_trips_{year}; just check if last")
                            break
        except Exception as e:
                    logging.error("Data insertion from Big Query failed: %s", e)
                    raise 
        
        

    # Load data into cloud storage
    def upload_to_gcs(bucket_name, local_path, gcs_path): # TODO: Check Implemention for GCP upload
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"Uploaded {local_path} to gs://{bucket_name}/{gcs_path}")

    def upload_to_aws(bucket_name, local_path, aws_path): # TODO: Implement AWS upload
        pass

    def upload_to_azure(bucket_name, local_path, azure_path): # TODO: Implement Azure upload
        pass


    ## Download and load data
    # Download files in the specified directory
    files_list = scrape_citibike_files()
    
    for url in files_list[-1:]: # TODO: Clear the list [-1:] to ingest all files in production or in cloud
        download_files(url) # Function does not return anything

    # Load data into postgres container
    load_data_to_postgres()
    ingest_from_bigquery_to_postgres()



if __name__ == '__main__':
    ## Define CLI arguments
    parser = argparse.ArgumentParser(description='Ingest CSV or Parquet data to posgres container')

    parser.add_argument('--user', required=False, help='user name for postgres', default='postgres')
    parser.add_argument('--password', required=False, help='password for postgres', default='postgres')
    parser.add_argument('--host', required=False, help='host for postgres', default='localhost')
    parser.add_argument('--port', required=False, help='port for postgres', default='5432')
    parser.add_argument('--db', required=False, help='database name for postgres', default='citibike')
    parser.add_argument('--table_name', required=False, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=False, help='url of the csv file')
    parser.add_argument('--download_dir', required=False, help='directory to download the csv file', default=DOWNLOAD_DIR)
    #parser.add_argument('--env', required=False, help='Deployment in Prod env or test in Dev env?', default=dev)
    #parser.add_argument('--chunk_size', required=False, help='Defines the chunk size to ingest', default=500_000) TODO: Implement chunk size and env arguments in CLI

    args = parser.parse_args()

    main(args)