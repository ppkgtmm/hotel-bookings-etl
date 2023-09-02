
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

COPY utilities/ ${DAGS_FOLDER}/utilities

COPY airflow/*.py ${DAGS_FOLDER}/


# install dependencies required for python
# RUN apk add wget gcc musl-dev make zlib-dev bash openssl-dev

# install python 3.9
# RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
# RUN tar xzf Python-${PYTHON_VERSION}.tgz
# RUN cd Python-${PYTHON_VERSION} && ./configure --enable-optimizations && make install && cd ../
# RUN rm -rf Python-${PYTHON_VERSION}.tgz Python-${PYTHON_VERSION}.tgz 


# install updated version of pip
# RUN python3 -m ensurepip && pip3 install --upgrade pip