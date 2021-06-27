require('dotenv').config()
const path = require('path')
const fs = require('fs')
const { runBenchmark } = require('./run-benchmark')
const logger = require('pino')()

;(async function main() {
  await runBenchmark(
    {
      connectionString: process.env.PGCONNECTIONSTRING
    },
    async pool => {
      logger.info({
        message: 'rebalance',
        'process.env.PGMAXCONN': process.env.PGMAXCONN,
        'process.env.DISPATCH_CONCURRENCY': process.env.DISPATCH_CONCURRENCY
      })

      await pool.query(
        String(
          fs.readFileSync(path.resolve(__dirname + '/../scripts/citus-rebalance.sql'), {
            encoding: 'utf8',
            flag: 'r'
          })
        )
      )
    }
  )
})()
