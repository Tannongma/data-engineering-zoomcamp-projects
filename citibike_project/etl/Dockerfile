FROM python:3.9

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip --default-timeout=120
RUN pip install --no-cache-dir --default-timeout=120 -r requirements.txt
RUN apt-get install wget

WORKDIR /app

#COPY ingest_data_citibike.py ingest_data_citibike.py


#ENTRYPOINT ["python", "ingest_data_citibike.py"]

COPY ingest_data.py ingest_data.py


ENTRYPOINT ["python", "ingest_data.py"]
