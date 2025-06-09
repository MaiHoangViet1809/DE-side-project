#!/bin/bash

docker-compose down
# kill -9 $(ps aux | grep "[.]listener" | awk '{print $2}')

docker-compose up --build --force-recreate -d --env-file ..\.env

echo "done";