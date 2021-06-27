const { runBenchmark } = require('./run-benchmark')
require('dotenv').config()
const logger = require('pino')()

;(async function main() {
  await runBenchmark(
    {
      connectionString: process.env.PGCONNECTIONSTRING
    },
    async pool => {
      const { rows } = await pool.query('SELECT NOW()')
      logger.info({
        rows
      })
    }
  )
})()
