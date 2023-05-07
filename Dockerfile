FROM python:3.9

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y postgresql-client


COPY .env .
COPY users.csv .
COPY create_db.py .
COPY load_api.py .
COPY reset_db.py .
COPY File_series.csv .


CMD ["python", "reset_db.py"]
