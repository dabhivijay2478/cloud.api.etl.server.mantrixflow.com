FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
# LOG_LEVEL env (default: warning) - set LOG_LEVEL=info in docker-compose for request logs
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port 8000 --log-level ${LOG_LEVEL:-warning}"]
