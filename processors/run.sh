#!/bin/sh

sleep 30

python3 date_dim.py

wget --directory-prefix=/ https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.33.tar.gz

tar -xf /mysql-connector-j-8.0.33.tar.gz -C /

python3 main.py
