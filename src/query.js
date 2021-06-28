require('dotenv').config()
const { isMainThread, parentPort, workerData } = require('worker_threads')
const logger = require('pino')()
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { worker: workerCount = 4, concurrency = 2000, maxDbConnection = 50, period = 30, query = 0 } = argv
const { runBenchmark } = require('./run-benchmark')
const { ReadQueries } = require('./sql/read-queries')
const { divideWorkFairly, timeElapsedInSecondsSince, Message } = require('./utils/utils')
const { startReportProgress } = require('./utils/start-report-progress')
const { runWorker } = require('./utils/run-worker')

if (isMainThread) {
  const workerStats = new Map()

  ;(async function main() {
    const stopReportProgress = startReportProgress({ workerStats, period })
    const concurrencyArr = divideWorkFairly(concurrency, workerCount)
    const maxDBConnectionArr = divideWorkFairly(maxDbConnection, workerCount)

    const jobs = new Array(workerCount).fill(null).map((_, index) =>
      runWorker({
        workerFilename: __filename,
        workerStats,
        workerId: index,
        concurrency: concurrencyArr[index],
        maxDbConnection: maxDBConnectionArr[index],
        period
      })
    )
    await Promise.all(jobs)

    stopReportProgress()
  })()
} else {
  // large enough to prevent query cache
  const paramCount = 3_000_000
  let processed = 0
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
        processed++
      } catch (e) {
        console.error(e)
        error++
      }
    }
  }

  ;(async function main() {
    const { workerId, concurrency = 2000, maxDbConnection = 50, period = 30 } = workerData

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

        // each worker should have different random seed
        ReadQueries.setRandomSeed(workerId)

        const jobMap = new Map([
          // support multiple jobs structures
          [0, () => [[ReadQueries.query0SQL, ReadQueries.query0Params(paramCount, Number(count))]]],
          [1, () => [[ReadQueries.query1SQL, ReadQueries.query1Params(paramCount, Number(count))]]],
          [2, () => [[ReadQueries.query2SQL, ReadQueries.query2Params(paramCount, Number(count))]]],
          [3, () => [[ReadQueries.query3SQL, ReadQueries.query3Params(paramCount, Number(count))]]],
          [4, () => [[ReadQueries.query4SQL, ReadQueries.query4Params(paramCount, Number(count))]]],
          [5, () => [[ReadQueries.query5SQL, ReadQueries.query5Params(paramCount, Number(count))]]]
        ])

        // create target job
        const targetJob = jobMap.get(Number(query))()

        const start = new Date().getTime()

        parentPort.postMessage(
          Message.createInitMessage({
            concurrency,
            maxDbConnection
          })
        )
        const reportProgressInterval = setInterval(() => {
          parentPort.postMessage(
            Message.createProgressMessage({
              processed,
              error,
              timeElapsedInSeconds: timeElapsedInSecondsSince(start)
            })
          )
        }, 1000)

        // benchmark for a specific period
        setTimeout(() => {
          finished = true
        }, period * 1000)

        const queryPs = new Array(Math.max(concurrency, 1)).fill(null).map(() => busyDispatcher(pool, targetJob))

        await Promise.all(queryPs)

        clearInterval(reportProgressInterval)

        parentPort.postMessage(
          Message.createDoneMessage({
            processed,
            error,
            timeElapsedInSeconds: timeElapsedInSecondsSince(start)
          })
        )
      }
    )
  })()
}
