FROM alpine:3.18.2

# install java 8
RUN apk add --no-cache openjdk8 wget gcc musl-dev make zlib-dev openssl-dev libffi-dev

ENV PYTHON_VERSION 3.9.6
# install python 3.9
RUN wget -O Python-${PYTHON_VERSION}.tgz https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz

RUN tar -xzf Python-${PYTHON_VERSION}.tgz && cd Python-${PYTHON_VERSION} && ./configure && make && make install

RUN cd ../ && rm -r Python-${PYTHON_VERSION} && rm Python-${PYTHON_VERSION}.tgz

# install required libraries
RUN apk add --no-cache bash librdkafka librdkafka-dev

RUN python3 -m ensurepip && pip3 install --upgrade pip

WORKDIR /processor

COPY requirements.txt .

# install required packages
RUN pip3 install --no-cache-dir -r requirements.txt

COPY *.py .

COPY dimensions/ dimensions/

COPY staging/ staging/
