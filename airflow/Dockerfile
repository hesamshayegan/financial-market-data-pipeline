FROM apache/airflow:2.10.2

# Copy requirements.txt to the image
COPY requirements.txt /requirements.txt

# Upgrade pip and install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
