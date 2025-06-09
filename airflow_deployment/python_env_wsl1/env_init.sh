#!/bin/bash

# declare function
export_file() {
  input=$1
  while IFS= read -r line
  do
    if [ -n "$line" ]; then
      if [[ $line =~ ^[^#] ]]; then
        echo "exporting $line"
        export "${line?}"
      fi
    fi
  done < "$input"
}

# unset all env exist in file
echo "-------------------- unset env exist in .env file"
unset $(grep -v '^#' .env |  cut -d '=' -f1 | xargs)
unset $(grep -v '^#' airflow_config.env |  cut -d '=' -f1 | xargs)

# set environment
echo "-------------------- set env exist in .env file"
#export $(grep -v '^#' .env | xargs -0)
#export $(grep -v '^#' airflow_config.env | xargs -0)
export_file ".env"
export_file "airflow_config.env"

#echo "--------------------------------------------------------------------------------------------------------------------------------"