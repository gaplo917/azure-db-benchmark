require('dotenv').config()
const { Pool } = require('pg')
const logger = require('pino')()
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { concurrency = 2000, maxDbConnection = 50, numOfQuerySet = 5000, query = 0 } = argv
const { ReadQueries } = require('./sql/read-queries')

let queried = 0
let error = 0

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
    try {
      await pool.query(query, param)
      queried++
    } catch (e) {
      console.error(e)
      error++
    }
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

  // release pool on quit
  ;['SIGINT', 'SIGTERM', 'SIGQUIT'].forEach(signal =>
    process.on(signal, () => {
      pool.end()
      process.exit(0)
    })
  )

  const { rows } = await pool.query(`SELECT count(*) FROM companies`)
  const { count } = rows[0]
  logger.info({
    message: `total ${count} companies`
  })

  const jobMap = new Map([
    // support multiple jobs structures
    [0, () => [[ReadQueries.query0SQL, ReadQueries.query0Params(numOfQuerySet)]]],
    [1, () => [[ReadQueries.query1SQL, ReadQueries.query1Params(numOfQuerySet, Number(count))]]],
    [2, () => [[ReadQueries.query2SQL, ReadQueries.query2Params(numOfQuerySet, Number(count))]]],
    [3, () => [[ReadQueries.query3SQL, ReadQueries.query3Params(numOfQuerySet, Number(count))]]],
    [4, () => [[ReadQueries.query4SQL, ReadQueries.query4Params(numOfQuerySet, Number(count))]]],
    [5, () => [[ReadQueries.query5SQL, ReadQueries.query5Params(numOfQuerySet, Number(count))]]]
  ])

  // create target job
  const targetJob = jobMap.get(Number(query))()

  const totalQueryCount = targetJob.reduce((acc, [_, params]) => acc + params.length, 0)

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
      error,
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
    error,
    timeElapsedInSeconds: getTimeElapsedInSeconds()
  })
  // release pool before exit
  pool.end()

  process.exit(0)
})()
