# hotel bookings etl

## Usage

#### Set up
1. Copy file `example.env` into a new file called `.env` in same directory
   
2. In `.env` file created, replace values that have following pattern `<FILL IN ...>` with suitable values 

3. Run following to allow set up script execution

```
chmod +x setup.sh
```

4. Run the set up script

```
./setup.sh
```

#### Execute project 

1. Activate virtual environment

```
source venv/bin/activate
```

2. Generate fake booking data

```
chmod +x generators/run.sh && ./generators/run.sh
```

3. Populate OLTP database using generated data
   
```
python3 seed_oltp.py
``` 

4. Create kafka topics in advance to prevent topic not found error

```
python3 kafka/create_topics.py 
```

5. Run first round of ETL in batch mode

```
chmod +x etl/batch/run.sh && ./etl/batch/run.sh
```

6. Register OLTP database to kafka connect

```
python3 kafka_connect/register_source.py
```

If the command above failed with connection error, retry after a few minutes

7. Copy SQL file to KSQL database container for execution
```
docker-compose cp ksql/window_events.sql ksqldb-server:/
```

8. Run the copied SQL file inside KSQL database container
```
docker-compose exec -it ksqldb-server /bin/sh -c "ksql http://0.0.0.0:${KSQL_SERVER_PORT} -f /window_events.sql"
```

9.  Start container for streaming ETL

```
docker-compose up -d processor --no-recreate
```

After a few minutes, data will start appearing in OLAP database

#### Run tests

Make sure that `processors/stream.py` start up, which will take time due to additional packages installation, is complete before testing

1. Activate virtual environment

```
source venv/bin/activate
```

2. update and delete records in OLTP database

```
python3 tests/update_oltp_records.py && python3 tests/delete_oltp_records.py
```

3. After 5 minutes (preferable), verify if the changes in OLTP database are synced to OLAP database

```
python3 tests/test_facts_updated.py &&  python3 tests/test_facts_deleted.py
```

There will not be any error messages printed out to console if tests were successful

#### Clean up
1. Run following after project execution to start airflow related containers for periodical clean up of staging tables

```
docker-compose up -d --no-recreate
```

2. Go to `http://localhost:8080/` in web browser then login to airflow with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` defined in `.env` file
   
3. Turn on the dag for clean up of staging tables in OLAP database


4. Click on the dag name to monitor it while running

Clean up dag is configured to run daily and will be triggered automatically

#### Tear down

```
docker-compose down -v
```
