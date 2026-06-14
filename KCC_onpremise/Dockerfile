FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_data.py .
COPY etl_pipeline.py .
COPY sql ./sql

CMD ["python", "etl_pipeline.py"]
