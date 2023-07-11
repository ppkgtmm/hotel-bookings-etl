# mochi

### Set up

```
chmod +x setup.sh && ./setup.sh
```

### Process
1. insert generated data to OLTP database
   
```
python3 seed_oltp.py
``` 

2. register database to kafka connect

```
python3 kafka_connect/register_mysql.py
```

### Tear down

```
docker-compose down
```
