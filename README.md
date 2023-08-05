# mochi

### Set up
1. Copy file `example.env` into a new file called `.env` in same directory
   
2. In `.env` file created, replace `<YOUR DB PASSWORD>` with wanted password 

3. Allow set up script execution

```
chmod +x setup.sh
```

### Process
1. Activate virtual environment

```
source venv/bin/activate
```

2. Generate fake hotel booking data (can skip after first run)

```
chmod +x generators/run.sh && ./generators/run.sh
```

3. run the set up script

```
./setup.sh
```

4. Insert generated data to OLTP database
   
```
python3 seed_oltp.py
``` 

5. You are all set ! after a few minutes, data will start appearing in OLAP database

### Tear down

```
docker-compose down -v
```
