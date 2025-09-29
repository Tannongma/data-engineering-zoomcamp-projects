FROM python:3.10-slim

WORKDIR /usr/app

# Install dbt and a specific adapter (e.g., dbt-postgres or dbt-bigquery)
# TODO: Add --no-cache-dir if image size becomes an issue or in prod to keep containers lean
COPY requirements.txt .
RUN pip install --upgrade pip --default-timeout=120
RUN pip install --default-timeout=120 -r requirements.txt


COPY . .

ENTRYPOINT ["dbt"]
