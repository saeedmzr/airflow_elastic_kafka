FROM apache/airflow:2.6.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential tzdata krb5-config libkrb5-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pendulum \
    requests \
    python-dotenv \
    elasticsearch \
    confluent_kafka


COPY --chown=airflow .env /opt/airflow/.env


USER root

ENV TZ=Asia/Tehran
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER airflow
