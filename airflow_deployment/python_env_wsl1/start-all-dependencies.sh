#!/usr/bin/bash

# --------------------------------------------------------------------------------------------------------------------------------
# check python venv and install if not exist
PATH_PYTHON_VENV="../../venv"
if [[ ! -d $PATH_PYTHON_VENV ]]; then
  # create python venv
  echo "creating python env 3.10"
  python3.10 -m venv $PATH_PYTHON_VENV
fi;

# source venv
source $PATH_PYTHON_VENV/bin/activate

# --------------------------------------------------------------------------------------------------------------------------------
# install project requirement libraries
echo "install project requirement libraries"
pip3.10 install -r ../../requirements.txt

# --------------------------------------------------------------------------------------------------------------------------------
# load all environment variables
echo "loading environment"
source env_init.sh

# --------------------------------------------------------------------------------------------------------------------------------
# install postgresql as backend for airflow
echo "install postgresql if server not exist"
if ! which psql; then

  sudo apt-get install wget ca-certificates gnupg2
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
  sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

  sudo apt-get update
  sudo apt-get install postgresql -y
  sudo service postgresql stop
  sudo service postgresql start

  # create db and user for airflow
  sudo -u postgres psql <<- EOSQL
  CREATE DATABASE airflow;
  CREATE USER airflow WITH PASSWORD 'airflow';
  GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
  -- PostgreSQL 15 requires additional privileges:
  -- USE airflow;
  GRANT ALL ON SCHEMA public TO airflow;
EOSQL

else
  sudo service postgresql stop
  sudo service postgresql start
fi;

# --------------------------------------------------------------------------------------------------------------------------------
# adapt localhost for postgresql backend of airflow (alternative for docker services name)
# this will make database uri consistent with docker-composer approach
grep -qxF '127.0.0.1   postgres-airflow' /etc/hosts || sudo -- sh -c -e "echo '127.0.0.1   postgres-airflow' >> /etc/hosts"

# dask
grep -qxF '127.0.0.1   dask-cluster' /etc/hosts || sudo -- sh -c -e "echo '127.0.0.1   dask-cluster' >> /etc/hosts"

# add entry to WSL1 to stop generate /etc/hosts else PC restart will need to run host
grep -qxF 'generateHosts = false' /etc/wsl.conf || sudo -- sh -c -e "echo 'generateHosts = false >> /etc/wsl.conf"

# --------------------------------------------------------------------------------------------------------------------------------
# create folder for airflow
echo "AIRFLOW_HOME=$AIRFLOW_HOME"
mkdir -p "$AIRFLOW_HOME"/logs
mkdir -p "$AIRFLOW_HOME"/plugins

# --------------------------------------------------------------------------------------------------------------------------------
# install java for pyspark
echo "install java"
sudo apt-get install openjdk-8-jdk

# --------------------------------------------------------------------------------------------------------------------------------
# start crontab service
sudo service cron start

# add git pull every minute
#(crontab -l 2>/dev/null; echo "*/1 * * * * git -C /home/production/EDW pull >> /home/production/git_output.txt 2>&1 ") | crontab -
sudo grep -qxF '*/1 * * * * git -C /home/production/EDW pull >> /home/production/git_output.txt 2>&1' /var/spool/cron/crontabs/production  \
  || sudo -- sh -c -e '(crontab -l 2>/dev/null; echo "*/1 * * * * git -C /home/production/EDW pull >> /home/production/git_output.txt 2>&1 ") | crontab -'

# start ssh service
# enable password authentication in ssh
sed -re 's/^(PasswordAuthentication)([[:space:]]+)no/\1\2yes/' -i.$(date -I) /etc/ssh/sshd_config
sudo /usr/bin/ssh-keygen -A
sudo service ssh start
