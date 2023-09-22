FROM alpine:3.18.2

# install java 8
RUN apk add --no-cache openjdk8 wget gcc musl-dev make zlib-dev openssl-dev libffi-dev

# install python 3.9
RUN wget -O Python-3.9.17.tgz https://www.python.org/ftp/python/3.9.17/Python-3.9.17.tgz

RUN tar -xzf Python-3.9.17.tgz && cd Python-3.9.17 && ./configure && make && make install

RUN cd ../ && rm -r Python-3.9.17

# install required libraries
RUN apk add --no-cache bash librdkafka librdkafka-dev

RUN python3 -m ensurepip && pip3 install --upgrade pip

WORKDIR /processor

COPY requirements.txt .

# install required packages
RUN pip3 install -r requirements.txt

COPY *.py .

COPY dimensions/ dimensions/

COPY staging/ staging/

ENTRYPOINT ["/bin/sh", "-c" ]
