FROM alpine:3.18.2

# install java 8
RUN apk add openjdk8

# install python 3.9
# cr. https://stackoverflow.com/questions/66221278/alpine-docker-define-specific-python-version-python3-3-8-7-r0breaks-worldpyt
RUN apk add python3=3.9.17-r0 --repository=http://dl-cdn.alpinelinux.org/alpine/v3.15/main

# install updated version of pip
RUN python3 -m ensurepip && pip3 install --upgrade pip

COPY . /processor
WORKDIR /processor

RUN apk add bash

# install required packages
RUN pip3 --no-cache-dir install -r requirements.txt

CMD [ "python3", "date_dim.py", "&&", "python3", "main.py" ]
