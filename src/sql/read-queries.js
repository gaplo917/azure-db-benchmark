const faker = require('faker')
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { randomSeed = 1 } = argv

faker.seed(randomSeed)

class ReadQueries {
  static query0SQL = `
    SELECT id
    FROM companies
    WHERE id = $1 AND created_at > $2 AND created_at < $3
    ORDER BY created_at
  `
  static query0Params = (workload, maxCompanyId) =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.datatype.number(maxCompanyId),
        faker.date.between('2015-01-01', '2018-01-01'),
        faker.date.between('2019-01-01', '2021-01-01')
      ]
    })

  static query1SQL = `
    SELECT id
    FROM campaigns
    WHERE company_id = $1 AND created_at > $2 AND created_at < $3 AND state = $4 AND monthly_budget > $5
    ORDER BY created_at
    LIMIT 100
  `
  static query1Params = (workload, maxCompanyId) =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.datatype.number(maxCompanyId),
        faker.date.between('2015-01-01', '2018-01-01'),
        faker.date.between('2019-01-01', '2021-01-01'),
        faker.address.state(),
        faker.datatype.number()
      ]
    })

  static query2SQL = `
    SELECT c.id, a.id
    FROM ads as a
    JOIN campaigns c
        ON c.company_id = a.company_id
               AND c.id = a.campaign_id
    WHERE c.company_id = $1 AND a.created_at > $2 AND a.created_at < $3 AND c.state = $4 AND c.monthly_budget > $5
    ORDER BY a.created_at
    LIMIT 100
  `
  static query2Params = (workload, maxCompanyId) =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.datatype.number(maxCompanyId),
        faker.date.between('2015-01-01', '2018-01-01'),
        faker.date.between('2019-01-01', '2021-01-01'),
        faker.address.state(),
        faker.datatype.number()
      ]
    })

  static query3SQL = `
    SELECT c.id, a.id, ca.id
    FROM clicks as c
    JOIN ads as a
        ON c.company_id = a.company_id
            AND c.ad_id = a.id
    JOIN campaigns ca
        ON ca.company_id = a.company_id
            AND ca.id = a.campaign_id
    WHERE c.company_id = $1 AND a.created_at > $2 AND a.created_at < $3 AND ca.state = $4 AND ca.monthly_budget > $5
    ORDER BY a.created_at
    LIMIT 100;
  `
  static query3Params = (workload, maxCompanyId) =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.datatype.number(maxCompanyId),
        faker.date.between('2015-01-01', '2018-01-01'),
        faker.date.between('2019-01-01', '2021-01-01'),
        faker.address.state(),
        faker.datatype.number()
      ]
    })

  // large amount of data
  static query4SQL = `
    SELECT i.id, a.id
    FROM impressions as i
    JOIN ads as a
        ON i.company_id = a.company_id
            AND i.ad_id = a.id
    WHERE a.company_id = $1 AND i.seen_at > $2 AND i.seen_at < $3
    ORDER BY i.seen_at
    LIMIT 100;
  `
  static query4Params = (workload, maxCompanyId) =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.datatype.number(maxCompanyId),
        faker.date.between('2015-01-01', '2018-01-01'),
        faker.date.between('2019-01-01', '2021-01-01')
      ]
    })

  // require table scan
  static query5SQL = `
    SELECT a.campaign_id,
           a.id,
           RANK() OVER (
               PARTITION BY a.campaign_id
               ORDER BY a.campaign_id, count(*) desc
           ),
           count(*) as n_impressions
    FROM ads as a
    JOIN impressions as i
        ON i.company_id = a.company_id
            AND i.ad_id = a.id
    WHERE a.company_id = $1 AND i.seen_at > $2 AND i.seen_at < $3
    GROUP BY a.campaign_id, a.id
    ORDER BY a.campaign_id, n_impressions desc
  `
  static query5Params = (workload, maxCompanyId) =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.datatype.number(maxCompanyId),
        faker.date.between('2015-01-01', '2018-01-01'),
        faker.date.between('2019-01-01', '2021-01-01')
      ]
    })
}
module.exports = { ReadQueries }
