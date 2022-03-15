#!/usr/bin/env bash
set -e

SCRIPT=$(readlink -f "$0")
SCRIPT_PATH=$(dirname "$SCRIPT")
DATA_PATH=$(realpath "${SCRIPT_PATH}"/data/)

for file in "$DATA_PATH"/*
do
  echo "Working with file ---> $file"

  docker exec -it ht_pg_server psql -U postgres -c "create database main_db"

  PYTHONPATH="${SCRIPT_PATH}" python3.7 "${SCRIPT_PATH}"/data_set_former.py  \
    --file_path $file \
    --output_path "${SCRIPT_PATH}"/data_set/ \
    --bd_config "${SCRIPT_PATH}"/bd_creator/db_config.yaml \
    --bd_schema "${SCRIPT_PATH}"/bd_creator/schemas.yaml \
    --max_text_len 200 \
    --batch_size 100000

  docker exec -it ht_pg_server psql -U postgres -c "drop database main_db"

done
