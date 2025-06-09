ARG AIRFLOW_VERSION
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.10

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential libsasl2-dev procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/app/requirements.txt


ARG RUN_AS_USER
USER $RUN_AS_USER