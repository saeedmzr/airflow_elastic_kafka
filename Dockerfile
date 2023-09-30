FROM apache/airflow:2.6.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential tzdata krb5-config libkrb5-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow requirements.txt /opt/airflow/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r requirements.txt


COPY --chown=airflow .env /opt/airflow/.env


USER root

ENV TZ=Asia/Tehran
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER airflow
