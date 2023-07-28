# mochi

### Set up
1. copy file `example.env` into a new file called `.env`
   
2. in `.env` file created, replace `<YOUR DB PASSWORD>` with wanted password 

3.  run set up script
```
chmod +x setup.sh && ./setup.sh
```

### Process
1. activate virtual environment

```
source venv/bin/activate
```

2. register database to kafka connect

```
python3 kafka_connect/register_mysql.py
```

3. insert generated data to OLTP database
   
```
python3 seed_oltp.py
``` 

### Tear down

```
docker-compose down -v
```
