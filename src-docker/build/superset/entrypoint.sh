#!/bin/bash
set -e

superset db upgrade
superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.org --password admin
superset init
superset run -h 0.0.0.0 -p 8088
