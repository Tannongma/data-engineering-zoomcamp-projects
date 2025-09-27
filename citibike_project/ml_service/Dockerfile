FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip --default-timeout=120
RUN pip install --no-cache-dir --default-timeout=120 -r requirements.txt

# Copy ML app and models
COPY app.py .
COPY models/ models/

# Expose the API port
EXPOSE 8000

# Run FastAPI app
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
