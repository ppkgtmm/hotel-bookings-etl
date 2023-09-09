FROM alpine:3.18.2

# install java 8
RUN apk add openjdk8

# install python 3.9
RUN apk add python3 python3-dev

# install updated version of pip
RUN python3 -m ensurepip && pip3 install --upgrade pip

# install required libraries
RUN apk add bash gcc musl-dev librdkafka librdkafka-dev

WORKDIR /processor

COPY etl/stream/requirements.txt .

# install required packages
RUN pip3 install -r requirements.txt

COPY etl/stream/stream.py scripts/

COPY utilities/*.py scripts/utilities/

ENTRYPOINT [ "/bin/sh", "-c" ]
