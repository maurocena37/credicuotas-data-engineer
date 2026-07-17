# PySpark in local mode needs a JVM. Slim Python base + a JDK is enough.
FROM python:3.11-slim

# Java (Spark 3.5 supports JDK 11/17) + procps for Spark scripts
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "src.pipeline"]
