# This module aims at experimenting locally 
# by creating containers to ingest a Citibike dataset for a given year

# Run the containers using the following docker command:
```sh
docker run -d \
	--name citibike-postgres \
    -e POSTGRES_USER=root \
	-e POSTGRES_PASSWORD=root \
	-e POSTGRES_DB=citibike \
    -e DB_PORT=5432 \
	-v $(pwd)/vol-postgres/data:/var/lib/postgresql/data \
	postgres:17-alpine
```
Or 

```sh 
docker-compose up -d
```

Then connect to pgcli to explore the database (empty at this moment)
```sh
pgcli -h localhost -p 5432 -u postgres -d citibike
```