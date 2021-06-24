## Azure DB benchmark
TBD


### Local development
```bash
# node 12 works, node 14 has some V8 bugs on JSON.stringify big js object on 'yarn data'
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

# (Optional) pre-generate lots of fake data to preview it
yarn data

# test connection
yarn ping

# benchmark insert
yarn insert

# benchmark query
yarn query
```

### Benchmark Client
```bash
# install node
curl https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash
source ~/.bashrc
nvm install 12.18.3
npm install -g yarn

# clone source
git clone https://github.com/gaplo917/azure-db-benchmark

cd azure-db-benchmark

# install dependencies
yarn

# create 
cp .env.local .env

# edit remote database
vi .env

# test connection
yarn ping

# init database
yarn reset 
# OR if it is a citus powered postgresql
yarn reset-citus

# benchmark insert, with 1 copy (2.2M records, ~1GB)
yarn insert 1

# benchmark query with base coefficient of workload
yarn query 50
```

### Advance Usage
if you need to run single instance is a bottleneck to benchmark your database, you need to run multiple benchmark instance
```bash
npm install -g pm2

# create pm2 config
mv insert.pm2.config.example.yaml insert.pm2.config.yaml

# start
pm2 start insert.pm2.config.yaml

# Show logs
pm2 logs
```
