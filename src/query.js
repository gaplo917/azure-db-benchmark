require('dotenv').config()
const { Pool } = require('pg')
const logger = require('pino')()
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { concurrency = 2000, maxDbConnection = 50, numOfQuerySet = 5000, intensity = 0 } = argv
const { ReadQueries } = require('./sql/read-queries')

const heavyQueryJobs = [[ReadQueries.heavyQuery1SQL, ReadQueries.heavyQuery1Params(numOfQuerySet)]]

const mediumQueryJobs = [[ReadQueries.heavyQuery2SQL, ReadQueries.heavyQuery2Params(numOfQuerySet)]]

const queryJobs = [
  [ReadQueries.query1SQL, ReadQueries.query1Params(numOfQuerySet)],
  [ReadQueries.query2SQL, ReadQueries.query2Params(numOfQuerySet)],
  [ReadQueries.query3SQL, ReadQueries.query3Params(numOfQuerySet)],
  [ReadQueries.query4SQL, ReadQueries.query4Params(numOfQuerySet)]
]

const sumCountReducer = (acc, [_, params]) => acc + params.length

const jobsSelection = intensity => {
  switch (intensity) {
    case 0:
    case '0':
      return queryJobs
    case 1:
    case '1':
      return mediumQueryJobs
    case 2:
    case '2':
      return heavyQueryJobs
  }
}
const targetJob = jobsSelection(intensity)
const totalQueryCount = targetJob.reduce(sumCountReducer, 0)

let queried = 0
let timeout = 0

const timeoutHandler = () => timeout++

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
    await pool.query(query, param).catch(timeoutHandler)
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

  const queryPs = new Array(Math.max(concurrency, 1))
    .fill(null)
    .map(() => busyDispatcher(pool, targetJob))

  await Promise.all(queryPs)

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
