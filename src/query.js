require('dotenv').config()
const faker = require('faker')
const { Pool } = require('pg')
const logger = require('pino')()
const workload = Number(process.argv[2]) || 50

// reproducible
faker.seed(1)

// require table scan
const heavyQuery1 = `
  SELECT a.campaign_id,
         RANK() OVER (
           PARTITION BY a.campaign_id
           ORDER BY a.campaign_id, count(*) desc
         ), count(*) as n_impressions, a.id
  FROM ads as a
         JOIN impressions as i
              ON i.company_id = a.company_id
                AND i.ad_id = a.id
  WHERE i.cost_per_impression_usd > $1
  GROUP BY a.campaign_id, a.id
  ORDER BY a.campaign_id, n_impressions desc
  LIMIT 100;
`
const heavyQuery1Params = new Array(workload).fill(null).map(() => {
  return [faker.datatype.number(1000) / 1000]
})

// large amount of data, but no need table scan
const heavyQuery2 = `
  SELECT i.*, a.name, a.target_url
  FROM impressions as i
         JOIN ads as a
              ON i.company_id = a.company_id
                AND i.ad_id = a.id
  WHERE i.cost_per_impression_usd > $1 AND i.seen_at > $2
  ORDER BY i.seen_at
  LIMIT 100;
`
const heavyQuery2Params = new Array(workload).fill(null).map(() => {
  return [faker.datatype.number(1000) / 1000, faker.date.between('2015-01-01', '2021-01-01')]
})

const query1 = `
  SELECT *
  FROM companies
  WHERE created_at > $1 AND created_at < $2
  ORDER BY created_at
  LIMIT 100
`
const coeffWorkloadQ1 = 400
const query1Params = new Array(workload * coeffWorkloadQ1).fill(null).map(() => {
  return [
    faker.date.between('2015-01-01', '2021-01-01'),
    faker.date.between('2015-01-01', '2021-01-01')
  ]
})

const query2 = `
  SELECT *
  FROM campaigns
  WHERE created_at > $1 AND created_at < $2 AND state = $3  AND monthly_budget > $4
  ORDER BY created_at
  LIMIT 100
`
const coeffWorkloadQ2 = 400
const query2Params = new Array(workload * coeffWorkloadQ2).fill(null).map(() => {
  return [
    faker.date.between('2015-01-01', '2021-01-01'),
    faker.date.between('2015-01-01', '2021-01-01'),
    faker.address.state(),
    faker.datatype.number()
  ]
})

const query3 = `
  SELECT *
  FROM ads as a
  JOIN campaigns c
      ON c.company_id = a.company_id
             AND c.id = a.campaign_id
  WHERE a.created_at > $1 AND a.created_at < $2
  ORDER BY a.created_at
  LIMIT 100
`
const coeffWorkloadQ3 = 400
const query3Params = new Array(workload * coeffWorkloadQ3).fill(null).map(() => {
  return [
    faker.date.between('2015-01-01', '2021-01-01'),
    faker.date.between('2015-01-01', '2021-01-01')
  ]
})

const query4 = `
  SELECT *
  FROM clicks as c
  JOIN ads as a
      ON c.company_id = a.company_id
             AND c.ad_id = a.id
  WHERE a.created_at > $1 AND c.cost_per_click_usd > $2
  ORDER BY c.cost_per_click_usd
  LIMIT 100
`
const coeffWorkloadQ4 = 20
const query4Params = new Array(workload * coeffWorkloadQ4).fill(null).map(() => {
  return [faker.date.between('2015-01-01', '2021-01-01'), faker.datatype.number(1000) / 1000]
})

const heavyQueryJobs = [
  [heavyQuery1, heavyQuery1Params],
  [heavyQuery2, heavyQuery2Params]
]

const queryJobs = [
  [query1, query1Params],
  [query2, query2Params],
  [query3, query3Params],
  [query4, query4Params]
]

const totalQueryCount =
  heavyQuery1Params.length +
  heavyQuery2Params.length +
  query1Params.length +
  query2Params.length +
  query3Params.length +
  query4Params.length

let queried = 0

async function busyDispatcher(pool, jobs) {
  let cursor = 0
  while (jobs.length > 0) {
    const index = cursor++ % jobs.length
    const [query, paramList] = jobs[index]
    if (paramList.length === 0) {
      // remove the job from job list
      jobs.splice(index, 1)
      continue
    }
    const param = paramList.pop()
    await pool.query(query, param)
    queried++
  }
}

async function query() {
  const concurrency = Number(process.env.DISPATCH_CONCURRENCY) || 2000
  const pool = new Pool({
    connectionString: process.env.PGCONNECTIONSTRING,
    max: Number(process.env.PGMAXCONN) || 50,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 300000,
    query_timeout: 3000000
  })
  await pool.connect()
  const start = new Date().getTime()
  logger.info({
    queried,
    progress: 0,
    totalQueryCount,
    queryRate: 0,
    timeElapsedInSeconds: 0
  })
  const getTimeElapsedInSeconds = () => Number((new Date().getTime() - start) / 1000).toFixed(2)
  const getProgress = () => Number(queried / totalQueryCount).toFixed(4)
  const getRate = () => Number(queried / getTimeElapsedInSeconds()).toFixed(2)
  const displayProgressInterval = setInterval(() => {
    logger.info({
      queried,
      progress: getProgress(),
      queryRate: `${getRate()}/s`,
      timeElapsedInSeconds: getTimeElapsedInSeconds()
    })
  }, 1000)

  // 5% dispatchers are heavy
  const numOfHeavyDispatcher = Math.max(Math.floor(concurrency * 0.05), 1)
  const heavyQueryPs = new Array(numOfHeavyDispatcher)
    .fill(null)
    .map(() => busyDispatcher(pool, heavyQueryJobs))

  const queryPs = new Array(Math.max(concurrency - numOfHeavyDispatcher, 1))
    .fill(null)
    .map(() => busyDispatcher(pool, queryJobs))

  await Promise.all([...heavyQueryPs, queryPs])

  clearInterval(displayProgressInterval)

  logger.info({
    queried,
    progress: getProgress(),
    queryRate: `${getRate()}/s`,
    timeElapsedInSeconds: getTimeElapsedInSeconds()
  })
  // release pool before exist
  pool.end()

  process.exit(0)
}

query()
