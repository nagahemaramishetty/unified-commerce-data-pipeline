# Dockerfile
#
# Extends the official Airflow image with dbt-postgres, so our DAG tasks can
# run `dbt run` / `dbt test` directly inside the Airflow container against
# our dbt_project, without needing dbt installed separately in every container.

FROM apache/airflow:2.9.3-python3.11

USER root

# PySpark needs a JVM. Installing a headless (no GUI) JDK keeps the image smaller.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.8.2 \
    dbt-postgres==1.8.2 \
    psycopg2-binary \
    pyspark==3.5.1

# Spark needs the Postgres JDBC driver to read/write Postgres directly. Downloaded once
# at build time and placed where pyspark looks for extra jars.
RUN curl -L -o /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
