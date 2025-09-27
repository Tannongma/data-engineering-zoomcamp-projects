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


import requests
import pandas as pd
from bs4 import BeautifulSoup

import sqlalchemy
from sqlalchemy import create_engine

from google.cloud import storage
#from azure.storage.blob import BlobServiceClient

# Local imports
#from safe_run import safe_run


## Set logging and configs
# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s')

# Connect to postgres database inside containter
engine = create_engine("postgresql://postgres:postgres@localhost:5432/citibike")
db_connection = engine.connect()


## Declare global variables
BASE_URL = "https://s3.amazonaws.com/tripdata/"
DOWNLOAD_DIR = "/home/bonaventure/Documents/data_engineering/data-engineering-zoomcamp-projects/2-docker_terraform/data/citibike_data"


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


def load_data_to_postgres(paths_list=find_csv_file(), engine=engine):
    """Create schema in psql database and load data"""
    for path in paths_list:
        df_name = "_".join(["citibike", str(path).split("/")[-2].strip()])
        df = pd.read_csv(filepath_or_buffer=path, 
                                   parse_dates=["started_at", "ended_at"])
        try:
            start_time = time()
            df.head(n=0).to_sql(name=df_name, con=engine, if_exists="replace")
            df.to_sql(name=df_name, con=engine, if_exists="append")
            end_time = time()
            print(f'Insertion of {df_name} complete, I/O osp time {(end_time-start_time):.2f}')
            logging.info(f"Insertion into postgres db complete: %s", df_name)
        except Exception as e:
            logging.error("Data insertion failed: %s", e)
            raise

# Load data into cloud storage
def upload_to_gcs(bucket_name, local_path, gcs_path): # TODO: Check Implemention for GCP upload
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{bucket_name}/{gcs_path}")

def upload_to_aws(bucket_name, local_path, gcs_path): # TODO: Implement AWS upload
    pass

def upload_to_azure(bucket_name, local_path, gcs_path): # TODO: Implement Azure upload
    pass


## Download and load data
# Download files in the specified directory
files_list = scrape_citibike_files()

for url in files_list[-1:]:
    download_files(url) # Function does not return anything

# Load data into postgres container
load_data_to_postgres()

