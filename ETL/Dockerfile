FROM bitnami/spark:3.5.5

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV HOME=/app

WORKDIR /app

RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ARG POSTGRES_JDBC_VERSION=42.7.3
RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    -o /opt/bitnami/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY etl_spark.py .

USER 1001

CMD ["spark-submit", "/app/etl_spark.py"]