
ARG PYTHON_VERSION
ARG AIRFLOW_VERSION

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG DAGS_FOLDER
ARG AIRFLOW_HOME

USER root

RUN mkdir -p ${AIRFLOW_HOME}

# cr. https://stackabuse.com/running-airflow-locally-with-docker-a-technical-guide/
RUN chown -R airflow ${AIRFLOW_HOME}
USER airflow

WORKDIR /

COPY airflow/requirements.txt .

# install required packages
RUN pip3 --no-cache-dir install -r requirements.txt

COPY airflow/setup.sh .

COPY scripts/common.py ${DAGS_FOLDER}/

COPY scripts/db_writer.py ${DAGS_FOLDER}/
