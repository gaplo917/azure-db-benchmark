const { runBenchmark } = require('./run-benchmark')
const { startReportProgress } = require('./utils/start-report-progress')
require('dotenv').config()
const logger = require('pino')()
const { isMainThread, parentPort, workerData } = require('worker_threads')
const { WriteQueries } = require('./sql/write-queries')
const { delay, divideWorkFairly, Message, timeElapsedInSecondsSince } = require('./utils/utils')
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { runWorker } = require('./utils/run-worker')

const { worker: workerCount = 4, concurrency = 2000, maxDbConnection = 40, numOfDataSet = 2000 } = argv
const DATASET_SIZE_LIMIT = 1000

if (isMainThread) {
  const { numOfRecords } = require('./generate-data')
  const workerStats = new Map()

  ;(async function main() {
    let unregisterReportProgress = null
    const totalRecords = numOfDataSet * numOfRecords

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
            await runWorker(
              {
                workerFilename: __filename,
                workerStats,
                workerId: `${index}-${dividedCount}`,
                concurrency: concurrencyArr[index],
                maxDbConnection: maxDBConnectionArr[index],
                numOfDataSet: dataSetSize
              },
              () => {
                if (unregisterReportProgress === null) {
                  unregisterReportProgress = startReportProgress({ workerStats, totalRecords })
                }
              }
            )
            dataSetLeft -= dataSetSize
            dividedCount++
          }
        })()
      })

      await Promise.all(jobs)
    } else {
      const jobs = new Array(workerCount).fill(null).map((_, index) =>
        runWorker(
          {
            workerFilename: __filename,
            workerStats,
            workerId: index,
            concurrency: concurrencyArr[index],
            maxDbConnection: maxDBConnectionArr[index],
            numOfDataSet: numOfDataSetArr[index]
          },
          () => {
            if (unregisterReportProgress === null) {
              unregisterReportProgress = startReportProgress({ workerStats, totalRecords })
            }
          }
        )
      )
      await Promise.all(jobs)
    }

    if (unregisterReportProgress) {
      unregisterReportProgress()
    }
  })()
} else {
  const { generateData, numOfRecords } = require('./generate-data')

  let processed = 0
  let error = 0

  const errorHandler = () => error++

  const busyDispatcher = async ({ pool, index, dataSet }) => {
    // write enough copy
    while (dataSet.length > 0) {
      const { company, campaign, ads, click, impression } = dataSet.pop()
      const pos0 = index % company.length
      const { rows: r0 } = await pool
        .query(WriteQueries.insertCompanySQL, WriteQueries.companyToQueryParam(company[pos0]))
        .catch(errorHandler)
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
          .catch(errorHandler)
        processed++
        const { id: campaignId } = r1[0]

        // insert ads
        const div1 = ads.length / campaign.length
        for (let j = 0; j < div1; j++) {
          const pos2 = pos1 * div1 + j
          const { rows: r2 } = await pool
            .query(WriteQueries.insertAdSQL, WriteQueries.adToQueryParam({ companyId, campaignId, ad: ads[pos2] }))
            .catch(errorHandler)
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
              .catch(errorHandler)
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
              .catch(errorHandler)
            processed++
          }
        }
      }
    }
  }

  ;(async function main() {
    const { workerId, concurrency = 2000, maxDbConnection = 50, numOfDataSet = 1 } = workerData
    await runBenchmark(
      {
        connectionString: process.env.PGCONNECTIONSTRING,
        max: maxDbConnection,
        idleTimeoutMillis: 30 * 1000,
        connectionTimeoutMillis: 60 * 1000,
        query_timeout: 5 * 60 * 1000
      },
      async pool => {
        // prepare data before any timing
        const dataSet = new Array(numOfDataSet).fill(null).map((_, index) => generateData(`${workerId}-${index}`))

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
              error,
              timeElapsedInSeconds: timeElapsedInSecondsSince(start)
            })
          )
        }, 1000)

        await Promise.all(new Array(concurrency).fill(null).map((_, index) => busyDispatcher({ pool, index, dataSet })))

        clearInterval(reportProgressInterval)

        // report the last status
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
