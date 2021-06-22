## Azure DB benchmark
TBD


### Local development
```bash
# node 12 works, node 14 has some V8 bugs on JSON.stringify big js object
nvm use 12.18.3

# init local postgresql
docker compose -f stack.yml up

# init local database, can use it to init remote database as well
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

# create .env
mv .env.local .env

# pre-generate lots of fake data to insert
yarn data

# benchmark insert
yarn insert-set1

# benchmark query
yarn query
```
