# mochi

### Set up
1. Copy file `example.env` into a new file called `.env` in same directory
   
2. In `.env` file created, replace `<YOUR DB PASSWORD>` with wanted password 

3. Allow set up script execution and run the set up script
```
chmod +x setup.sh && ./setup.sh
```

### Process
1. Activate virtual environment

```
source venv/bin/activate
```

2. Register OLTP database to kafka connect

```
python3 kafka_connect/register_mysql.py
```

3. Insert generated data to OLTP database
   
```
python3 seed_oltp.py
``` 

4. You are all set ! after a few minutes, data will start appearing in OLAP database

### Tear down

```
docker-compose down -v
```
