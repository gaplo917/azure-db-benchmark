const aggregateProcessed = stats => {
  return stats
    .map(it => it.processed)
    .reduce((acc, e) => acc + e, 0)
    .toFixed(2)
}

const aggregateTotalTimeout = stats => {
  return stats
    .map(it => it.timeout)
    .reduce((acc, e) => acc + e, 0)
    .toFixed(2)
}
const aggregateTimeUsed = stats => {
  return stats
    .map(it => it.timeElapsedInSeconds)
    .reduce((acc, e) => acc + e, 0)
    .toFixed(2)
}
const calcProgress = ({ stats, totalRecords }) => {
  return Number(aggregateProcessed(stats) / totalRecords).toFixed(4)
}
const calcAvgRate = stats => {
  const minStartAt = stats.map(it => it.startedAt).reduce((acc, e) => Math.min(acc, e), 0)
  const maxEndAt =
    stats.map(it => it.endedAt).reduce((acc, e) => Math.max(acc, e), 0) || new Date().getTime()
  return Number((aggregateProcessed(stats) / (maxEndAt - minStartAt)) * 1000).toFixed(2)
}

module.exports = {
  aggregateTimeUsed,
  aggregateProcessed,
  aggregateTotalTimeout,
  calcProgress,
  calcAvgRate
}
