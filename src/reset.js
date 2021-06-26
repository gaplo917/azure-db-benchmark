require('dotenv').config()
const { Pool } = require('pg')
const path = require('path')
const fs = require('fs')
const logger = require('pino')()
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { citus } = argv

async function ping() {
  const pool = new Pool({
    connectionString: process.env.PGCONNECTIONSTRING,
    max: process.env.PGMAXCONN,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 30000
  })
  await pool.connect()

  logger.info({
    message: 'reset',
    'process.env.PGMAXCONN': process.env.PGMAXCONN,
    'process.env.DISPATCH_CONCURRENCY': process.env.DISPATCH_CONCURRENCY
  })

  await pool.query(
    String(
      fs.readFileSync(path.resolve(__dirname + '/../scripts/init.sql'), {
        encoding: 'utf8',
        flag: 'r'
      })
    )
  )
  if (citus) {
    await pool.query(
      String(
        fs.readFileSync(path.resolve(__dirname + '/../scripts/citus-shard.sql'), {
          encoding: 'utf8',
          flag: 'r'
        })
      )
    )
  }

  // release pool before exist
  pool.end()

  process.exit(0)
}

ping()
