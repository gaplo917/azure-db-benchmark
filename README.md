## PostgreSQL & Citus Scaling Benchmark
Simple data analytic use case to test data sharding in Citus-powered PostgreSQL.

Using node.js clients to insert/query PostgreSQL database and check the result.

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

# benchmark query, query={0,1,2,3,4,5,6}
yarn query --query=0
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

# init database (drop all tables and create again)
yarn reset 
# OR if it is a citus powered postgresql, do an extra step for data sharding
yarn reset-citus

# Re-balance citus distributed table after scaling database
yarn rebalance

# insert reproducible random data with 4000 dataset (2.2M records, ~1.2GB)
yarn insert --worker=4 --concurrency=2000 --maxDbConnection=250 --numOfDataSet=4000

# insert reproducible random data with 240000 dataset (269M records, ~75GB), will divide and ramp-up workers internally
yarn insert --worker=8 --concurrency=8000 --maxDbConnection=500 --numOfDataSet=240000

# query={0,1,2,3} light workload weight query(all hit index with random parameters)
# query={4} heavy workload query (all hit index with random parameters but large amount of data join)
# query={5} heavy workload query (table scan and large amount of data join)
yarn query --query=0 --worker=4 --concurrency=8000 --maxDbConnection=250 --period=180 > output/q0.txt \
&& sleep 5s \
&& yarn query --query=1 --worker=4 --concurrency=8000 --maxDbConnection=250 --period=180 > output/q1.txt \
&& sleep 5s \
&& yarn query --query=2 --worker=4 --concurrency=8000 --maxDbConnection=250 --period=180 > output/q2.txt \
&& sleep 5s \
&& yarn query --query=3 --worker=4 --concurrency=4000 --maxDbConnection=250 --period=180 > output/q3.txt \
&& sleep 5s \
&& yarn query --query=4 --worker=4 --concurrency=4000 --maxDbConnection=250 --period=180 > output/q4.txt \
&& sleep 5s \
&& yarn query --query=5 --worker=4 --concurrency=4000 --maxDbConnection=250 --period=180 > output/q5.txt

# query={6} test Citus distributed tables aggregate overhead
yarn query --query=6 --worker=4 --concurrency=2000 --maxDbConnection=250 --period=180 > output/q6.txt
```
