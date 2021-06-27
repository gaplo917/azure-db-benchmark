## Azure DB benchmark
TBD


### Local development
```bash
# tested on node 14
nvm use 14

# init local postgresql
docker compose -f stack.yml up

# create .env
mv .env.local .env

# init local database, can use it to init remote database as well
yarn reset

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
nvm install 14
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

# insert reproducible random data with 4000 dataset (2.2M records, ~1.2GB)
yarn insert --worker=4 --concurrency=2000 --maxDbConnection=250 --numOfDataSet=4000

# insert reproducible random data with 240000 dataset (269M records, ~75GB), will divide and ramp-up workers internally
yarn insert --worker=8 --concurrency=8000 --maxDbConnection=500 --numOfDataSet=240000

# 4 light workload weight query(all hit index with random parameters), 25000 numOfQuerySet (100k queries)
yarn query --intensity=0  --concurrency=2000 --maxDbConnection=250 --numOfQuerySet=25000
yarn query --intensity=1  --concurrency=2000 --maxDbConnection=250 --numOfQuerySet=25000
yarn query --intensity=2  --concurrency=2000 --maxDbConnection=250 --numOfQuerySet=25000
yarn query --intensity=3  --concurrency=2000 --maxDbConnection=250 --numOfQuerySet=25000

# 1 medium workload query (all hit index with random parameters but large amount of data join), 10000 numOfQuerySet (10k queries)
yarn query --intensity=4  --concurrency=1000 --maxDbConnection=250 --numOfQuerySet=10000

# 1 heavy workload query (table scan and large amount of data join), 50 numOfQuerySet (50 query)
yarn query --intensity=5 --concurrency=4 --maxDbConnection=250 --numOfQuerySet=50

```
