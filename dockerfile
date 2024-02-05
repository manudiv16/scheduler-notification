FROM python:3.12.1

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev

RUN pip install --no-cache-dir -r requirements.txt

COPY src .

EXPOSE 8000

CMD ["python", "runner.py"]