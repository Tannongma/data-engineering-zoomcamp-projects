# Use Bitnami Spark base image
FROM quay.io/jupyter/base-notebook:latest


# Set working directory
WORKDIR /app


# Copy requirements.txt early to leverage caching
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies in one step
# TODO: Add --no-cache-dir if image size becomes an issue or in prod to keep containers lean
RUN pip install --upgrade pip --default-timeout=120 \
    && pip install --default-timeout=120 -r /tmp/requirements.txt 



# Set default command to start jupyterlab
ENTRYPOINT ["bash", "jupyter"]