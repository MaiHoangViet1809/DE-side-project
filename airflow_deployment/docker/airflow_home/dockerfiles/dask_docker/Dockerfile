FROM --platform=linux/amd64 python:3.10.5-slim

USER root

# add java to image
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1
ENV JAVA_HOME=""

# install addition package
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends build-essential libsasl2-dev musl-dev openssh-client unixodbc-dev \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# setup global env
ENV DASK_HOST=localhost
ENV DASK_PORT=8786
ENV AIRFLOW_HOME=/opt/airflow

# expose port
EXPOSE 8786 4040
EXPOSE 10000-50000

# install sql server driver 17
RUN apt-get update \
&& apt-get install -y curl apt-transport-https gnupg

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update \
&& ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
&& apt-get autoremove -yqq --purge \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

# install requirements python packages
ARG FRAMEWORK_HOME
COPY requirements.txt /opt/app/requirements.txt
COPY packages /opt/airflow/packages
RUN pip install --no-cache-dir -r /opt/app/requirements.txt --find-links /opt/airflow/packages

# set current workdir
WORKDIR /opt/airflow

# create user
ARG RUN_AS_USER
RUN useradd -rm -d /home/airflow -s /bin/bash -g root -G sudo -u $RUN_AS_USER airflow
RUN chown airflow:0 /opt/airflow
USER airflow
