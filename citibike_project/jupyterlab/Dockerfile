# Use Jupyter base image
FROM quay.io/jupyter/base-notebook:latest

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install dependencies
RUN pip install --upgrade pip --default-timeout=120 \
    && pip install --default-timeout=120 -r /tmp/requirements.txt

# Environment variables for Spark
ENV SPARK_MASTER=spark://spark-master:7077 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=jupyter \
    PYSPARK_DRIVER_PYTHON_OPTS="notebook"
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy code at the end (changes won’t break cache above)
COPY . /app
WORKDIR /app