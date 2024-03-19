# 🗺️ hotel bookings etl

data warehousing on realistic hotel reservation data through batch and real-time processing

## initialization

required only for the first time you are running the project

1. copy file `example.env` into a new file called `.env` in project directory
   
2. in the `.env` file, fill in values that have following pattern `<TO BE FILLED>` as needed 

3. run following to allow permit helper script execution

```
chmod +x run.sh
```

## set up

1. creates a virtual environment if not exist and installs packages

2. starts containers required to execute the project

3. initializes transactional database and data warehouse

```
./run.sh setup
```

## data generation

generates synthetic data related to hotel reservations e.g. users, rooms, bookings, amenities etc.

```
./run.sh datagen
```

## data population

populates transactional database with synthetic data


```
./run.sh seed
```


## stream processing

1. registers connector to capture and send changes in transactional database to kafka

2. populates location and date dimension tables with initial dataset

3. creates spark processor to consume data from kafka

4. spark consumer captures every change in dimension tables but retains only latest state of fact related data

```
./run.sh etl
```

if the command fails because of `AssertionError` or `connection issue`, retry after some time


## batch processing

1. run command below to start airflow scheduler and webserver

```
./run.sh airflow
```

2. after a few minutes, go to http://localhost:8080 in web browser then login to airflow website with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` defined in `.env` file
   
3. turn on the following pipelines : clean_up, process_facts, process_full_picture

4. optionally, click on any pipeline name to monitor it while running

upon successful completion
- fact tables and full picture table will be populated
- staging tables will be cleaned up to reduce storage consumption


## Testing

tests fact tables population logic to verify if 
- bookings or amenities data are correctly inserted and
- data is inserted only when less than 7 days is left before checkin date

```
./run.sh test
```

test is considered as failed when  `AssertionError` is thrown



## Tear down

stops and removes containers in use by the project 

```
./run.sh down
```

## References

- [wikipedia-activity-diagram](https://en.wikipedia.org/wiki/Activity_diagram)
- [database-diagram-tool](https://dbdiagram.io)
- [data-warehouse-online-course](https://www.udemy.com/share/106qIm/)
- [fake-data-generator-documentation](https://faker.readthedocs.io/en/master/)
- [sql-alchemy-documentation](https://docs.sqlalchemy.org/en/20/)
- [debezium-mysql-documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [debezium-mysql-privileges-stackoverflow](https://stackoverflow.com/questions/70658178/how-to-grant-all-mysql-8-0-privileges-to-debezium-in-windows)
- [debezium-avro-documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html)
- [reading-confluent-avro-with-spark](https://medium.com/@mrugankray/real-time-avro-data-analysis-with-spark-streaming-and-confluent-kafka-in-python-426f5e05392d)
- [spark-kafka-integration](https://spark.apache.org/docs/3.4.0/structured-streaming-kafka-integration.html)
- [spark-package-specification](https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated)
- [pyspark-3.4.0-documentation](https://spark.apache.org/docs/3.4.0/api/python/index.html)
- [install-python-on-alpine-linux](https://stackoverflow.com/a/73294721)
- [guide-for-running-airflow-with-docker](https://stackabuse.com/running-airflow-locally-with-docker-a-technical-guide/)
- [creating-airflow-connections-from-cli](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-from-the-cli)
- [airflow-templates-and-macros-documentation](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
- [airflow-external-task-sensor-documentation](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/external_task/index.html#airflow.sensors.external_task.ExternalTaskSensor)
- [working-with-json-data-in-mysql](https://www.digitalocean.com/community/tutorials/working-with-json-in-mysql)
- [unnest-extract-nested-json-data-in-mysql](https://andreessulp.medium.com/how-to-unnest-extract-nested-json-data-in-mysql-8-0-c9322c90df12)
- [booking-dashboard-tableau-public](https://public.tableau.com/views/HotelBookingDemand_16990387653270/bookingdemand?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link)
- [addon-dashboard-tableau-public](https://public.tableau.com/views/HotelAddonDemand/addondemand?:language=en-US&:display_count=n&:origin=viz_share_link)

