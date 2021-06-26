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

# benchmark insert, with 1 copy (2.2M records, ~800MB)
yarn insert --worker=1 --concurrency=2000 --maxDbConnection=50 --numOfCopies=1

# benchmark query with base coefficient of workload
yarn query --worker=1 --concurrency=2000 --maxDbConnection=50 --workload=50
```
