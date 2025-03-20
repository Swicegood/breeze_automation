FROM python:3.8

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY breezeapi.py /app/pyBreezeChMS/breezeapi.py

CMD ["python", "main.py"]
