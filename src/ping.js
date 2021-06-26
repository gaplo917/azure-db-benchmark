require('dotenv').config()
const { Pool } = require('pg')
const logger = require('pino')()

;(async function main() {
  const pool = new Pool({
    connectionString: process.env.PGCONNECTIONSTRING
  })
  await pool.connect()

  const { rows } = await pool.query('SELECT NOW()')

  logger.info({
    rows
  })
  // release pool before exist
  pool.end()

  process.exit(0)
})()
