airflow db init

airflow users create --username admin --password ${AIRFLOW_ADMIN_PASSWORD} --firstname First --lastname Last --role Admin --email admin@example.com

airflow connections add ${AIRFLOW_OLAP_CONN_ID} --conn-uri "mysql://${DB_USER}:${DB_PASSWORD}@${DB_HOST_INTERNAL}:${DB_PORT}/${OLAP_DB}"
