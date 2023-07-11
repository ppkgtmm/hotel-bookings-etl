# mochi

### Set up

```
chmod +x setup.sh && ./setup.sh
```

### Process
1. activate venv

```
source venv/bin/activate
```

2. insert generated data to OLTP database
   
```
python3 seed_oltp.py
``` 

3. register database to kafka connect

```
python3 kafka_connect/register_mysql.py
```

### Tear down

```
docker-compose down
```
