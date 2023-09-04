FROM alpine:3.18.2

# install java 8
RUN apk add openjdk8

# install python 3.9
# cr. https://stackoverflow.com/questions/66221278/alpine-docker-define-specific-python-version-python3-3-8-7-r0breaks-worldpyt
RUN apk add python3=3.9.17-r0 --repository=http://dl-cdn.alpinelinux.org/alpine/v3.15/main

# required by confluent-kafka
RUN apk add python3-dev=3.9.17-r0 --repository=http://dl-cdn.alpinelinux.org/alpine/v3.15/main

# install updated version of pip
RUN python3 -m ensurepip && pip3 install --upgrade pip

# install required libraries
RUN apk add bash gcc musl-dev librdkafka librdkafka-dev

WORKDIR /processor

COPY requirements.txt .

# install required packages
RUN pip3 --no-cache-dir install -r requirements.txt

COPY run.sh .

COPY kafka/*.py .

COPY kafka_connect/*.py .

COPY *.py scripts/

COPY utilities/*.py scripts/utilities/

COPY ksql/ ksql/

ENTRYPOINT [ "/bin/sh", "-c" ]
