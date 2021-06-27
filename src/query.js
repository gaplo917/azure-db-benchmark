const { timeElapsedInSecondsSince } = require('./utils/utils')
const { runBenchmark } = require('./run-benchmark')
require('dotenv').config()
const logger = require('pino')()
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { concurrency = 2000, maxDbConnection = 50, period = 60, query = 0 } = argv
const { ReadQueries } = require('./sql/read-queries')

const randomCount = 3_000_000
let queried = 0
let error = 0
let finished = false

async function busyDispatcher(pool, jobs) {
  let cursor = 0
  while (!finished && jobs.length > 0) {
    const index = cursor++ % jobs.length
    const [query, paramList] = jobs[index]
    const param = paramList[cursor % paramList.length]
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
  await runBenchmark(
    {
      connectionString: process.env.PGCONNECTIONSTRING,
      max: Number(maxDbConnection),
      idleTimeoutMillis: 30 * 1000,
      connectionTimeoutMillis: 5 * 60 * 1000,
      query_timeout: 5 * 60 * 1000
    },
    async pool => {
      const { rows } = await pool.query(`SELECT count(*) FROM companies`)
      const { count } = rows[0]
      logger.info({
        message: `total ${count} companies`
      })

      const jobMap = new Map([
        // support multiple jobs structures
        [0, () => [[ReadQueries.query0SQL, ReadQueries.query0Params(randomCount)]]],
        [1, () => [[ReadQueries.query1SQL, ReadQueries.query1Params(randomCount, Number(count))]]],
        [2, () => [[ReadQueries.query2SQL, ReadQueries.query2Params(randomCount, Number(count))]]],
        [3, () => [[ReadQueries.query3SQL, ReadQueries.query3Params(randomCount, Number(count))]]],
        [4, () => [[ReadQueries.query4SQL, ReadQueries.query4Params(randomCount, Number(count))]]],
        [5, () => [[ReadQueries.query5SQL, ReadQueries.query5Params(randomCount, Number(count))]]]
      ])

      // create target job
      const targetJob = jobMap.get(Number(query))()

      const start = new Date().getTime()
      logger.info({
        queried,
        progress: 0,
        queryRate: 0,
        timeElapsedInSeconds: 0
      })
      const getProgress = () =>
        Math.min(1, Number(timeElapsedInSecondsSince(start) / period)).toFixed(4)
      const getRate = () => Number(queried / timeElapsedInSecondsSince(start)).toFixed(2)
      const displayProgressInterval = setInterval(() => {
        logger.info({
          queried,
          progress: getProgress(),
          queryRate: `${getRate()}/s`,
          error,
          timeElapsedInSeconds: timeElapsedInSecondsSince(start)
        })
      }, 1000)

      // benchmark for a specific period
      setTimeout(() => {
        finished = true
      }, period * 1000)

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
        timeElapsedInSeconds: timeElapsedInSecondsSince(start)
      })
    }
  )
})()
