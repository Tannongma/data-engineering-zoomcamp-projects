FROM apache/airflow:2.9.3-python3.10

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Install extra dependencies for Postgres, Spark, etc.
COPY requirements.txt .
RUN pip install --upgrade pip --default-timeout=120
RUN pip install --default-timeout=120 -r requirements.txt

# Copy DAGs into the container
COPY dags/ ${AIRFLOW_HOME}/dags/

# Default command to run airflow
CMD ["airflow", "standalone"]
# CMD ["airflow", "webserver"]