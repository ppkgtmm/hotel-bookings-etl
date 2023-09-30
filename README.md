# hotel bookings etl
Repo created to store source code of data engineering project with primary focus on ETL of hotel reservation data from transactional database to analytical database. Work invloved range from data source preparation, data modeling, change data capture configuration, data processing logic development and testing

## Project definition
Primary focus of this project is to do ETL on bookings data generated from a hotel website that does not exist in reality. In order to determine data attributes required to be present in source database, possible functionalities of the imaginary website were listed down as follows
- registered users should be able to reserve available hotel rooms on specified dates
- registered users should be able to make reservations for preferred and available room types
- registered users should be able to make reservation for themselves or on behalf of registered guests
- registered users should be able to purchase amenities or addons for convenience

Some constraints of the imaginary hotel website are
- registered users must be registered as guests before making reservation for themselves
- one registered guest can be tied to only one room in case of multiple rooms reserved in a booking
- one registered guest cannot have multiple bookings with overlapping dates
- room reservation, purchase of amenities, cancelling and updating details of booking or amenities cannot be done later than 1 week prior to checkin date



## Data modeling

## Usage

Make sure to be inside project directory in your terminal

#### Initialization

Required only for the first time you are running the project

1. Copy file `example.env` into a new file called `.env` in the same directory
   
2. In `.env` file created, fill in values that have following pattern `<TO BE FILLED>` as needed 

3. Run following to allow helper script execution

```
chmod +x run.sh
```

#### Set up

With this step, virtual environment is created (if not exist) and libraries are installed. Also, containers required to execute the project are started. Eventually, transactional and analytical databases are initialized

```
./run.sh setup
```

#### Data generation

Fake data related to hotel reservations e.g. users, rooms, bookings, amenities etc. are generated by running the command below

```
./run.sh datagen
```

#### Data population

After running commend below, data generated in previous step is populated to transactional database

```
./run.sh seed
```

#### ETL - Part 1

In this step, insert, update or delete operations inside transactional database is captured by connector from database log file and sent to kafka broker for further consumption by stream processer. Furthermore, some dimesions namely location and date dimesnions are populated

```
./run.sh etl
```


- Every change in dimension related tables are captured while only latest state of fact related tables are retained in staging area
- If the command fails because of `AssertionError` or `connection issue`, retry after some time

#### ETL - Part 2 

1. Run command below to start airflow scheduler and webserver
```
./run.sh airflow
```

2. After a few minutes, go to `http://localhost:8080/` in web browser then login to airflow website with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` defined in `.env` file
   
3. Turn on the following pipelines or dags
   - clean_up
   - process_facts

4. Optionally, click on any dag name to monitor it while running

Upon successful completion of the pipeliness, fact tables will be populated and staging area will be cleaned up to reduce storage consumption

#### Run tests
```
./run.sh test
```
Firstly, test fact data yet to be processed i.e. bookings or amenities with at least 7 days different from current date are populated in staging tables. Then, airflow dag which processes facts

#### Tear down

With this step, containers in use by project are stopped and removed

```
./run.sh down
```
