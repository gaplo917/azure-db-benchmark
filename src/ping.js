require('dotenv').config()
const { Pool } = require('pg')
const logger = require('pino')()

async function ping() {
  const pool = new Pool({
    connectionString: process.env.PGCONNECTIONSTRING,
    max: process.env.PGMAXCONN,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 30000
  })
  await pool.connect()

  const { rows } = await pool.query('SELECT NOW()')

  logger.info({
    rows,
    'process.env.PGMAXCONN': process.env.PGMAXCONN,
    'process.env.DISPATCH_CONCURRENCY': process.env.DISPATCH_CONCURRENCY
  })
  // release pool before exist
  pool.end()

  process.exit(0)
}

ping()
