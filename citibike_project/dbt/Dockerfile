FROM python:3.10-slim

WORKDIR /usr/app

# Install dbt and a specific adapter (e.g., dbt-postgres or dbt-bigquery)
COPY requirements.txt .
RUN pip install --upgrade pip --default-timeout=120
RUN pip install --no-cache-dir --default-timeout=120 -r requirements.txt


COPY . .

ENTRYPOINT ["dbt"]
