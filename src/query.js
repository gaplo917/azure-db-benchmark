require('dotenv').config()
const faker = require('faker')
const { Pool } = require('pg')
const logger = require('pino')()
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { worker: workerCount = 1, concurrency = 2000, maxDbConnection = 50, workload = 50 } = argv
const { ReadQueries } = require('./sql/read-queries')

// reproducible
faker.seed(1)

const heavyQueryJobs = [
  [ReadQueries.heavyQuery1SQL, ReadQueries.heavyQuery1Params(workload)],
  [ReadQueries.heavyQuery2SQL, ReadQueries.heavyQuery2Params(workload)]
]

const queryJobs = [
  [ReadQueries.query1SQL, ReadQueries.query1Params(workload * 400)],
  [ReadQueries.query2SQL, ReadQueries.query2Params(workload * 400)],
  [ReadQueries.query3SQL, ReadQueries.query3Params(workload * 100)],
  [ReadQueries.query4SQL, ReadQueries.query4Params(workload * 25)]
]

const sumCountReducer = (acc, [_, params]) => acc + params.length
const totalQueryCount =
  heavyQueryJobs.reduce(sumCountReducer, 0) + queryJobs.reduce(sumCountReducer, 0)

let queried = 0

async function busyDispatcher(pool, jobs) {
  let cursor = 0
  while (jobs.length > 0) {
    const index = cursor++ % jobs.length
    const [query, paramList] = jobs[index]
    if (paramList.length === 0) {
      // remove the job from job list
      jobs.splice(index, 1)
      continue
    }
    const param = paramList.pop()
    await pool.query(query, param)
    queried++
  }
}

;(async function main() {
  const pool = new Pool({
    connectionString: process.env.PGCONNECTIONSTRING,
    max: Number(maxDbConnection),
    idleTimeoutMillis: 30 * 1000,
    connectionTimeoutMillis: 60 * 1000,
    query_timeout: 5 * 60 * 1000
  })
  await pool.connect()
  const start = new Date().getTime()
  logger.info({
    queried,
    progress: 0,
    totalQueryCount,
    queryRate: 0,
    timeElapsedInSeconds: 0
  })
  const getTimeElapsedInSeconds = () => Number((new Date().getTime() - start) / 1000).toFixed(2)
  const getProgress = () => Number(queried / totalQueryCount).toFixed(4)
  const getRate = () => Number(queried / getTimeElapsedInSeconds()).toFixed(2)
  const displayProgressInterval = setInterval(() => {
    logger.info({
      queried,
      progress: getProgress(),
      queryRate: `${getRate()}/s`,
      timeElapsedInSeconds: getTimeElapsedInSeconds()
    })
  }, 1000)

  // 5% dispatchers are heavy
  const numOfHeavyDispatcher = Math.max(Math.floor(concurrency * 0.05), 1)
  const heavyQueryPs = new Array(numOfHeavyDispatcher)
    .fill(null)
    .map(() => busyDispatcher(pool, heavyQueryJobs))

  const queryPs = new Array(Math.max(concurrency - numOfHeavyDispatcher, 1))
    .fill(null)
    .map(() => busyDispatcher(pool, queryJobs))

  await Promise.all([...heavyQueryPs, ...queryPs])

  clearInterval(displayProgressInterval)

  logger.info({
    queried,
    progress: getProgress(),
    queryRate: `${getRate()}/s`,
    timeElapsedInSeconds: getTimeElapsedInSeconds()
  })
  // release pool before exist
  pool.end()

  process.exit(0)
})()
