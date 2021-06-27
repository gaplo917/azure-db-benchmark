const { Pool } = require('pg')

async function runBenchmark(poolConfig, success) {
  const pool = new Pool(poolConfig)

  await success(pool)

  // release pool before exist
  await pool.end()
  process.exit(0)
}

module.exports = { runBenchmark }
