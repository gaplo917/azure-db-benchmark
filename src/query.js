require('dotenv').config()
const faker = require('faker')
const { Pool } = require('pg')
const logger = require('pino')()
const { ReadQueries } = require('./sql/read-queries')

// reproducible
faker.seed(1)

const heavyQueryJobs = [
  [ReadQueries.heavyQuery1SQL, ReadQueries.heavyQuery1Params],
  [ReadQueries.heavyQuery2SQL, ReadQueries.heavyQuery2Params]
]

const queryJobs = [
  [ReadQueries.query1SQL, ReadQueries.query1Params],
  [ReadQueries.query2SQL, ReadQueries.query2Params],
  [ReadQueries.query3SQL, ReadQueries.query3Params],
  [ReadQueries.query4SQL, ReadQueries.query4Params]
]

const totalQueryCount =
  ReadQueries.heavyQuery1Params.length +
  ReadQueries.heavyQuery2Params.length +
  ReadQueries.query1Params.length +
  ReadQueries.query2Params.length +
  ReadQueries.query3Params.length +
  ReadQueries.query4Params.length

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

async function query() {
  const concurrency = Number(process.env.DISPATCH_CONCURRENCY) || 2000
  const pool = new Pool({
    connectionString: process.env.PGCONNECTIONSTRING,
    max: Number(process.env.PGMAXCONN) || 50,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 300000,
    query_timeout: 3000000
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

  await Promise.all([...heavyQueryPs, queryPs])

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
}

query()
