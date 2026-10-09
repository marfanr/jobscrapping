FROM apache/airflow:3.0.6-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        wget \
        ca-certificates \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

# Copy folder Spark langsung dari image Spark official (instan, hitungan detik)
COPY --from=apache/spark:4.1.3-scala2.13-java17-python3-r-ubuntu /opt/spark /opt/spark

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Iceberg
RUN wget -q \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    -O ${SPARK_HOME}/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar

RUN wget -q \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar \
    -O ${SPARK_HOME}/jars/iceberg-aws-bundle-1.11.0.jar


ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-docker \
    apache-airflow-providers-fab \
    airflow-dbt-python \
    pandas \
    pyarrow \ 
    spacy \
    dbt-trino \
    astronomer-cosmos \
    pyahocorasick
