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

1. run command below to start airflow scheduler and web server

```
./run.sh airflow
```

2. after a few minutes, go to http://localhost:8080 in web browser then login to airflow website with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` defined in `.env` file
   
3. turn on the following pipelines : clean_up, process_facts, process_full_picture

4. optionally, click on any pipeline name to monitor it while running

upon successful completion
- fact tables and full picture table will be populated
- staging tables will be cleaned up to reduce storage consumption


## testing

tests fact tables population logic to verify if 
- bookings or amenities data are correctly inserted and
- data is inserted only when less than 7 days is left before checkin date

```
./run.sh test
```

test is considered as failed when  `AssertionError` is thrown



## tear down

stops and removes containers in use by the project 

```
./run.sh down
```
