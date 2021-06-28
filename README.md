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

# query={0,1,2,3} light workload weight query(all hit index with random parameters)
# query={4} medium workload query (all hit index with random parameters but large amount of data join)
# query={5} heavy workload query (table scan and large amount of data join)
yarn query --query=0 --worker=8 --concurrency=8000 --maxDbConnection=250 --period=180 > output/q0.txt \
&& sleep 5s \
&& yarn query --query=1 --worker=8 --concurrency=8000 --maxDbConnection=250 --period=180 > output/q1.txt \
&& sleep 5s \
&& yarn query --query=2 --worker=8 --concurrency=8000 --maxDbConnection=250 --period=180 > output/q2.txt \
&& sleep 5s \
&& yarn query --query=3 --worker=4 --concurrency=2000 --maxDbConnection=250 --period=180 > output/q3.txt \
&& sleep 5s \
&& yarn query --query=4 --worker=4 --concurrency=2000 --maxDbConnection=250 --period=180 > output/q4.txt \
&& sleep 5s \
&& yarn query --query=5 --worker=4 --concurrency=2000 --maxDbConnection=250 --period=180 > output/q5.txt

```
