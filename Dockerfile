FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates templates

ENV APP_DB=/data/app.db \
    PORT=8000
RUN mkdir -p /data

EXPOSE 8000

# 多 worker + 多线程下, SQLite 的 BEGIN IMMEDIATE 事务保证并发不超支
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "8", "--timeout", "60", "app:app"]
