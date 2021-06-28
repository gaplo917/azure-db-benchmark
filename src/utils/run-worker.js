const { Worker } = require('worker_threads')
const logger = require('pino')()
const { Message } = require('./utils')

function runWorker(workerData, onWorkerInit) {
  const { workerFilename, workerStats, workerId } = workerData
  return new Promise((resolve, reject) => {
    logger.info({
      message: 'create new worker',
      workerData
    })
    const worker = new Worker(workerFilename, {
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
          onWorkerInit && onWorkerInit(worker)
          break
        case Message.PROGRESS:
          workerStats.set(workerId, {
            ...workerStats.get(workerId),
            error: Number(payload.error),
            processed: Number(payload.processed),
            timeElapsedInSeconds: Number(payload.timeElapsedInSeconds)
          })
          break
        case Message.DONE:
          workerStats.set(workerId, {
            ...workerStats.get(workerId),
            isDone: true,
            endedAt: new Date().getTime(),
            error: Number(payload.error),
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

module.exports = { runWorker }
