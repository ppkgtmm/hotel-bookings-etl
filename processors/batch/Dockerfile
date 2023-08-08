FROM apache/airflow:2.3.3-python3.9

ENV AIRFLOW_HOME /airflow

USER root

RUN mkdir -p ${AIRFLOW_HOME}

# cr. https://stackabuse.com/running-airflow-locally-with-docker-a-technical-guide/
RUN chown -R airflow ${AIRFLOW_HOME} 
USER airflow

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor

WORKDIR /
COPY setup.sh .
