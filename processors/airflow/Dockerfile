FROM apache/airflow:2.3.3-python3.9

ENV AIRFLOW_HOME /airflow
ENV DAGS_FOLDER ${AIRFLOW_HOME}/dags
ENV AIRFLOW__CORE__EXECUTOR LocalExecutor
ENV AIRFLOW__CORE__DAGS_FOLDER ${DAGS_FOLDER}

USER root

RUN mkdir -p ${AIRFLOW_HOME}

# cr. https://stackabuse.com/running-airflow-locally-with-docker-a-technical-guide/
RUN chown -R airflow ${AIRFLOW_HOME} 
USER airflow

WORKDIR /

COPY airflow/setup.sh .
COPY utilities/ ${DAGS_FOLDER}/scripts/utilities
COPY airflow/*.py ${DAGS_FOLDER}/
