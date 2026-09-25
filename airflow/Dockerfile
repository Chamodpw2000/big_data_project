# Airflow image with Java + PySpark, so the DAG can run the batch job in-process.
# Alternative designs (SparkSubmitOperator against a cluster, or DockerOperator
# talking to the host daemon) add moving parts we do not need at this scale.
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1