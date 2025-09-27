# Use official Python slim image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy job code
COPY app/ ./app
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip --default-timeout=120
RUN pip install --no-cache-dir --default-timeout=120 -r requirements.txt

# Set environment for Spark submit
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Entry point: you can override in docker-compose or Kubernetes job
ENTRYPOINT ["python", "app/my_spark_job.py"]
