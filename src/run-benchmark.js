const { Pool } = require('pg')

async function runBenchmark(poolConfig, success) {
  const pool = new Pool(poolConfig)
  // release on quit
  ;['SIGINT', 'SIGTERM', 'SIGQUIT'].forEach(signal =>
    process.on(signal, async () => {
      await pool.end()
      process.exit(0)
    })
  )

  await success(pool)

  // release pool before exist
  await pool.end()
  process.exit(0)
}

module.exports = { runBenchmark }
