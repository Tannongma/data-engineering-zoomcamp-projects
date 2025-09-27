# Use Bitnami Spark base image
FROM bitnami/spark:3.5.0

# Switch to root to install packages
USER root

# Set working directory
WORKDIR /opt/bitnami/spark

# ARG for version flexibility
ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_AWS_VERSION=3.3.1
ARG AWS_SDK_VERSION=1.11.901
ARG POSTGRES_JDBC_VERSION=42.7.2

# Install OS dependencies
RUN apt-get update && apt-get install -y \
    wget curl unzip python3-pip openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt early to leverage caching
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies in one step
RUN pip install --upgrade pip --default-timeout=120 \
    && pip install --no-cache-dir --default-timeout=120 -r /tmp/requirements.txt

# Create a directory for Spark jars if it doesn't exist
RUN mkdir -p /opt/bitnami/spark/jars

# Download Iceberg runtime JAR
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar \
    -P /opt/bitnami/spark/jars/

# Download Hadoop AWS + AWS SDK for S3 support
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -P /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -P /opt/bitnami/spark/jars/

# Download PostgreSQL JDBC driver
RUN wget -q https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    -P /opt/bitnami/spark/jars/

# Environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SPARK_CLASSPATH=$SPARK_HOME/jars/*

# Switch back to non-root for security
USER 1001

# Set default command to start Spark shell
#CMD ["spark-shell"]