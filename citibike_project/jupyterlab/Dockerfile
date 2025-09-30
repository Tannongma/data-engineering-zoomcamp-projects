# Use Jupyter base image
FROM quay.io/jupyter/base-notebook:latest


# Install dependencies
RUN pip install --upgrade pip --default-timeout=120 
# Step 1: install stable core deps
COPY base-requirements.txt /tmp/base-requirements.txt
RUN pip install --default-timeout=300 -i https://pypi.org/simple -r /tmp/base-requirements.txt

# Step 2: install project-specific deps (rebuilds more often)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --default-timeout=300 -i https://pypi.org/simple -r /tmp/requirements.txt


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