## Azure DB benchmark
TBD


### Local development
```bash
docker compose -f stack.yml up
```

### Init Database
```bash
docker run \
-it \
-e PGPASSWORD=example \
-v $(pwd)/scripts:/app/scripts \
--rm \
postgres \
psql \
-h host.docker.internal \
-p 5432 \
-U postgres \
-a \
-f /app/scripts/init.sql
```

### Run Benchmark
```bash
# pre-generate fake data for later insert
yarn data
```
