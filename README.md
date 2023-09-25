# hotel bookings etl

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

#### Run ETL - Part 1

In this step, insert, update or delete operations inside transactional database is captured by connector from database log file and sent to kafka broker for further consumption by stream processer. Furthermore, some dimesions namely location and date dimesnions are populated

```
./run.sh etl
```


Every change in dimension related tables are captured while only latest state of fact related tables are retained in staging area

#### Run ETL - Part 2

1. Run command below to start airflow scheduler and webserver
```
./run.sh airflow
```

2. After a few minutes, go to `http://localhost:8080/` in web browser then login to airflow website with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` defined in `.env` file
   
3. Turn on the following pipelines or dags
   - clean_up
   - process_dims
   - process_facts

4. Optionally, click on any dag name to monitor it while running

Fact tables are now populated after ensuring existence of required dimesions and staging area is cleaned up to reduce storage consumption upon successful completion of the pipelines triggered daily

#### Tear down

```
./run.sh down
```
