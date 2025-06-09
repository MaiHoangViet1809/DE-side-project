#!/bin/bash

# active python virtual environment
PATH_PYTHON_VENV="../../venv"
source $PATH_PYTHON_VENV/bin/activate

# load all environment variables
source env_init.sh

# create tables core for framework
python3 "${FRAMEWORK_HOME}"/cores/logic_repos/init_framework.py

# create DE Automation user
if [ -d "$USERNAME_AIRFLOW_DE_AUTOMATION" ]; then
  echo "Creating DE Automation user in airflow"
  airflow users create --username "$USERNAME_AIRFLOW_DE_AUTOMATION" --firstname DE --lastname Automation --role Admin --email DE.Automation@kcc.com --password "$PASSWORD_AIRFLOW_DE_AUTOMATION"
fi

