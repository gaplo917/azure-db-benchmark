const logger = require('pino')()

const {
  aggregateTimeUsed,
  aggregateProcessed,
  aggregateTotalTimeout,
  calcProgress,
  calcAvgRate
} = require('./worker-stat')

const { timeElapsedInSecondsSince } = require('./utils')

const startReportProgress = ({ totalRecords, workerStats }) => {
  const start = new Date().getTime()
  let lastRecord = {
    totalProcessed: 0,
    timeElapsedInSeconds: 0
  }
  const interval = setInterval(() => {
    if (workerStats.size === 0) {
      return
    }
    const stats = Array.from(workerStats.values())

    const data = {
      totalProcessed: aggregateProcessed(stats),
      totalTimeout: aggregateTotalTimeout(stats),
      totalTimeUsed: aggregateTimeUsed(stats),
      progress: calcProgress({ stats, totalRecords }),
      avgProcessRate: `${calcAvgRate(stats)}/s`,
      timeElapsedInSeconds: timeElapsedInSecondsSince(start)
    }
    const processedDiff = data.totalProcessed - lastRecord.totalProcessed
    const timeElapsedDiff = data.timeElapsedInSeconds - lastRecord.timeElapsedInSeconds

    logger.info({
      ...data,
      currentProcessRate: Number(processedDiff / timeElapsedDiff).toFixed(2)
    })
    lastRecord = {
      totalProcessed: data.totalProcessed,
      timeElapsedInSeconds: data.timeElapsedInSeconds
    }
  }, 1000)

  return () => {
    const stats = Array.from(workerStats.values())

    logger.info({
      totalProcessed: aggregateProcessed(stats),
      totalTimeout: aggregateTotalTimeout(stats),
      totalTimeUsed: aggregateTimeUsed(stats),
      progress: calcProgress({ stats, totalRecords }),
      avgProcessRate: `${calcAvgRate(stats)}/s`,
      timeElapsedInSeconds: timeElapsedInSecondsSince(start)
    })
    clearInterval(interval)
  }
}

module.exports = { startReportProgress }
