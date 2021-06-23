require('dotenv').config()
const faker = require('faker')
const { Pool } = require('pg')
const logger = require('pino')()
const workload = Number(process.argv[2]) || 10000

// reproducible
faker.seed(1)

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

const heavyQuery2 = `
  SELECT i.*, a.name, a.target_url
  FROM impressions as i
         JOIN ads as a
              ON i.company_id = a.company_id
                AND i.ad_id = a.id
  WHERE i.cost_per_impression_usd > $1 AND i.seen_at > $2
  ORDER BY i.cost_per_impression_usd, i.seen_at
  LIMIT 100;
`
const heavyQuery2Params = new Array(workload).fill(null).map(() => {
  return [faker.datatype.number(1000) / 1000, faker.date.between('2015-01-01', '2021-01-01')]
})

const query1 = `
  SELECT *
  FROM companies
  WHERE created_at > $1 AND created_at < $2
  LIMIT 100
`
const query1Params = new Array(workload * 10).fill(null).map(() => {
  return [
    faker.date.between('2015-01-01', '2021-01-01'),
    faker.date.between('2015-01-01', '2021-01-01')
  ]
})

const query2 = `
  SELECT *
  FROM campaigns
  WHERE created_at > $1 AND created_at < $2 AND state = $3  AND monthly_budget > $4
  LIMIT 100
`
const query2Params = new Array(workload * 10).fill(null).map(() => {
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
  LIMIT 100
`
const query3Params = new Array(workload * 5).fill(null).map(() => {
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
  LIMIT 100
`
const query4Params = new Array(workload * 5).fill(null).map(() => {
  return [faker.date.between('2015-01-01', '2021-01-01'), faker.datatype.number(1000) / 1000]
})

const queryJobs = [
  [heavyQuery1, heavyQuery1Params],
  [heavyQuery2, heavyQuery2Params],
  ...new Array(10).fill([query1, query1Params]),
  ...new Array(10).fill([query2, query2Params]),
  ...new Array(5).fill([query3, query3Params]),
  ...new Array(5).fill([query4, query4Params])
]

const totalQueryCount =
  heavyQuery1Params.length +
  heavyQuery2Params.length +
  query1Params.length +
  query2Params.length +
  query3Params.length +
  query4Params.length

let queried = 0
let cursor = 0

async function busyDispatcher(pool) {
  while (queryJobs.length > 0) {
    const index = cursor++ % queryJobs.length
    const [query, paramList] = queryJobs[index]
    if (paramList.length === 0) {
      // remove the job from job list
      queryJobs.splice(index, 1)
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

  await Promise.all(new Array(concurrency).fill(null).map(() => busyDispatcher(pool)))

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
