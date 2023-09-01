# hotel bookings etl

## Usage

#### Set up
1. Copy file `example.env` into a new file called `.env` in same directory
   
2. In `.env` file created, replace `<YOUR DB PASSWORD>` with wanted password 

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

4. Register OLTP database to kafka connect

```
python3 kafka_connect/register_mysql.py
```

5. Start container for initial load and streaming ETL

```
docker-compose up -d processor
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

#### Tear down

```
docker-compose down -v
```
