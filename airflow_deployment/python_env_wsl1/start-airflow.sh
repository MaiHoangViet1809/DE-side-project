#!/bin/bash

# --------------------------------------------------------------------------------------------------------------------------------
# start/install dependencies service
source start-all-dependencies.sh

# --------------------------------------------------------------------------------------------------------------------------------
# active python virtual environment
PATH_PYTHON_VENV="../../venv"
source $PATH_PYTHON_VENV/bin/activate

# load all environment variables
source env_init.sh

# --------------------------------------------------------------------------------------------------------------------------------
# start dask cluster
if pgrep "dasks*" -a; then
  sudo pkill "dasks*";
  sleep 5
fi
cd "${FRAMEWORK_HOME}"/airflow_home || (echo "fail to cd into airflow home" && exit)
touch .dask_output
nohup dask scheduler >> .dask_output 2>&1 &
sleep 3
nohup dask worker tcp://localhost:8786 >> .dask_output 2>&1 &

# --------------------------------------------------------------------------------------------------------------------------------
# start airflow instance
# kill old process if alive
if pgrep "airflow" -xa; then
  sudo pkill "airflow" -x;
fi

# remove file pid
if [ -n "$AIRFLOW_HOME" ]; then
  find "$AIRFLOW_HOME"/ -maxdepth 1 -name "*.pid" -delete
fi

# --------------------------------------------------------------------------------------------------------------------------------
# start airflow up
airflow scheduler -D
airflow webserver -D
airflow triggerer -D
