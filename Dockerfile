FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY prod.py /app/prod.py

COPY src /app/src

EXPOSE 4545

CMD ["python", "prod.py"]
