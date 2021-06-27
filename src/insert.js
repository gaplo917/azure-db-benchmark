const { startReportProgress } = require('./utils/start-report-progress')
require('dotenv').config()
const logger = require('pino')()
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads')
const { WriteQueries } = require('./sql/write-queries')
const { delay, divideWorkFairly, Message, timeElapsedInSecondsSince } = require('./utils/utils')
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const {
  worker: workerCount = 4,
  concurrency = 2000,
  maxDbConnection = 40,
  numOfDataSet = 2000
} = argv
const DATASET_SIZE_LIMIT = 1000

if (isMainThread) {
  const { numOfRecords } = require('./generate-data')
  const workerStats = new Map()
  function runWorker({ workerId, concurrency, maxDbConnection, numOfDataSet }) {
    return new Promise((resolve, reject) => {
      const workerData = {
        workerId,
        concurrency: concurrency,
        maxDbConnection: maxDbConnection,
        numOfDataSet
      }
      logger.info({
        message: 'create new worker',
        workerData
      })
      const worker = new Worker(__filename, {
        workerData
      })
      worker.on('message', ({ type, payload }) => {
        switch (type) {
          case Message.INIT:
            workerStats.set(workerId, {
              isDone: false,
              startedAt: new Date().getTime(),
              processed: 0,
              timeElapsedInSeconds: 0
            })
            logger.info({
              message: 'new worker join',
              workerPayload: payload
            })
            break
          case Message.PROGRESS:
            workerStats.set(workerId, {
              ...workerStats.get(workerId),
              timeout: Number(payload.timeout),
              processed: Number(payload.processed),
              timeElapsedInSeconds: Number(payload.timeElapsedInSeconds)
            })
            break
          case Message.DONE:
            workerStats.set(workerId, {
              ...workerStats.get(workerId),
              isDone: true,
              endedAt: new Date().getTime(),
              timeout: Number(payload.timeout),
              processed: Number(payload.processed),
              timeElapsedInSeconds: Number(payload.timeElapsedInSeconds)
            })
            logger.info({
              message: 'worker done',
              workerId,
              workerPayload: payload
            })
            break
          default:
            logger.warn({
              message: 'unsupported message type:' + type
            })
        }
      })
      worker.on('error', reject)
      worker.on('exit', code => {
        if (code !== 0) {
          logger.error(new Error(`Worker stopped with exit code ${code}`))
        }
        resolve()
      })
    })
  }

  ;(async function main() {
    const totalRecords = numOfDataSet * numOfRecords

    const stopReportProgress = startReportProgress({ workerStats, totalRecords })

    const concurrencyArr = divideWorkFairly(concurrency, workerCount)
    const maxDBConnectionArr = divideWorkFairly(maxDbConnection, workerCount)
    const numOfDataSetArr = divideWorkFairly(numOfDataSet, workerCount)

    if (numOfDataSetArr.find(it => it > DATASET_SIZE_LIMIT)) {
      logger.info({
        message: `data size too large (>${DATASET_SIZE_LIMIT}), going to divide and ramp-up workers`
      })
      const jobs = new Array(workerCount).fill(null).map((_, index) => {
        let dataSetLeft = numOfDataSetArr[index]
        let dividedCount = 0
        return (async () => {
          // 15s ramp up delay to prevent all workers start/finish at the same time for CPU intensive stuffs
          await delay(index * 15 * 1000)

          while (dataSetLeft > 0) {
            const dataSetSize = dataSetLeft > DATASET_SIZE_LIMIT ? DATASET_SIZE_LIMIT : dataSetLeft
            await runWorker({
              workerId: `${index}-${dividedCount}`,
              concurrency: concurrencyArr[index],
              maxDbConnection: maxDBConnectionArr[index],
              numOfDataSet: dataSetSize
            })
            dataSetLeft -= dataSetSize
            dividedCount++
          }
        })()
      })

      await Promise.all(jobs)
    } else {
      const jobs = new Array(workerCount).fill(null).map((_, index) =>
        runWorker({
          workerId: index,
          concurrency: concurrencyArr[index],
          maxDbConnection: maxDBConnectionArr[index],
          numOfDataSet: numOfDataSetArr[index]
        })
      )
      await Promise.all(jobs)
    }

    stopReportProgress()
  })()
} else {
  const { Pool } = require('pg')
  const { generateData, numOfRecords } = require('./generate-data')

  let processed = 0
  let timeout = 0

  const timeoutHandler = () => timeout++

  const busyDispatcher = async ({ pool, index, dataSet }) => {
    // write enough copy
    while (dataSet.length > 0) {
      const { company, campaign, ads, click, impression } = dataSet.pop()
      const pos0 = index % company.length
      const { rows: r0 } = await pool
        .query(WriteQueries.insertCompanySQL, WriteQueries.companyToQueryParam(company[pos0]))
        .catch(timeoutHandler)
      processed++
      const { id: companyId } = r0[0]

      // insert campaign
      const div0 = campaign.length / company.length
      for (let i = 0; i < div0; i++) {
        const pos1 = pos0 * div0 + i
        const { rows: r1 } = await pool
          .query(
            WriteQueries.insertCampaignSQL,
            WriteQueries.campaignToQueryParam({ companyId, campaign: campaign[pos1] })
          )
          .catch(timeoutHandler)
        processed++
        const { id: campaignId } = r1[0]

        // insert ads
        const div1 = ads.length / campaign.length
        for (let j = 0; j < div1; j++) {
          const pos2 = pos1 * div1 + j
          const { rows: r2 } = await pool
            .query(
              WriteQueries.insertAdSQL,
              WriteQueries.adToQueryParam({ companyId, campaignId, ad: ads[pos2] })
            )
            .catch(timeoutHandler)
          processed++
          const { id: adId } = r2[0]

          // insert click
          const div2 = click.length / ads.length
          for (let k = 0; k < div2; k++) {
            const pos3 = pos2 * div2 + k
            await pool
              .query(
                WriteQueries.insertClicksSQL,
                WriteQueries.clickToQueryParam({
                  companyId,
                  adId,
                  click: click[pos3]
                })
              )
              .catch(timeoutHandler)
            processed++
          }

          // insert impression
          const div3 = impression.length / ads.length
          for (let k = 0; k < div3; k++) {
            const pos3 = pos2 * div3 + k
            await pool
              .query(
                WriteQueries.insertImpressionSQL,
                WriteQueries.impressionToQueryParam({
                  companyId,
                  adId,
                  impression: impression[pos3]
                })
              )
              .catch(timeoutHandler)
            processed++
          }
        }
      }
    }
  }

  ;(async function main() {
    const { workerId, concurrency = 2000, maxDbConnection = 50, numOfDataSet = 1 } = workerData
    const pool = new Pool({
      connectionString: process.env.PGCONNECTIONSTRING,
      max: maxDbConnection,
      idleTimeoutMillis: 30 * 1000,
      connectionTimeoutMillis: 60 * 1000,
      query_timeout: 5 * 60 * 1000
    })
    await pool.connect()

    // prepare data before any timing
    const dataSet = new Array(numOfDataSet)
      .fill(null)
      .map((_, index) => generateData(`${workerId}-${index}`))

    const start = new Date().getTime()

    parentPort.postMessage(
      Message.createInitMessage({
        totalRecordCount: numOfDataSet * numOfRecords,
        concurrency,
        maxDbConnection
      })
    )

    // report
    const reportProgressInterval = setInterval(() => {
      parentPort.postMessage(
        Message.createProgressMessage({
          processed,
          timeout,
          timeElapsedInSeconds: timeElapsedInSecondsSince(start)
        })
      )
    }, 1000)

    await Promise.all(
      new Array(concurrency).fill(null).map((_, index) => busyDispatcher({ pool, index, dataSet }))
    )

    clearInterval(reportProgressInterval)

    // report the last status
    parentPort.postMessage(
      Message.createDoneMessage({
        processed,
        timeout,
        timeElapsedInSeconds: timeElapsedInSecondsSince(start)
      })
    )

    // release pool before exit
    pool.end()
    process.exit(0)
  })()
}
