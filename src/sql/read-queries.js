const faker = require('faker')
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { randomSeed = 1 } = argv

faker.seed(randomSeed)

// require table scan
class ReadQueries {
  static heavyQuery1SQL = `
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
  static heavyQuery1Params = workload =>
    new Array(workload).fill(null).map(() => {
      return [faker.datatype.number(1000) / 1000]
    })

  // large amount of data, but no need table scan
  static heavyQuery2SQL = `
    SELECT i.id, a.id
    FROM impressions as i
           JOIN ads as a
                ON i.company_id = a.company_id
                  AND i.ad_id = a.id
    WHERE i.cost_per_impression_usd > $1 AND i.seen_at > $2
    ORDER BY i.seen_at
    LIMIT 100;
  `
  static heavyQuery2Params = workload =>
    new Array(workload).fill(null).map(() => {
      return [faker.datatype.number(1000) / 1000, faker.date.between('2015-01-01', '2021-01-01')]
    })

  static query1SQL = `
    SELECT id
    FROM companies
    WHERE created_at > $1 AND created_at < $2
    ORDER BY created_at
    LIMIT 100
  `
  static query1Params = workload =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.date.between('2015-01-01', '2021-01-01'),
        faker.date.between('2015-01-01', '2021-01-01')
      ]
    })

  static query2SQL = `
    SELECT id
    FROM campaigns
    WHERE created_at > $1 AND created_at < $2 AND state = $3  AND monthly_budget > $4
    ORDER BY created_at
    LIMIT 100
  `
  static query2Params = workload =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.date.between('2015-01-01', '2021-01-01'),
        faker.date.between('2015-01-01', '2021-01-01'),
        faker.address.state(),
        faker.datatype.number()
      ]
    })

  static query3SQL = `
    SELECT c.id, a.id
    FROM ads as a
    JOIN campaigns c
        ON c.company_id = a.company_id
               AND c.id = a.campaign_id
    WHERE a.created_at > $1 AND a.created_at < $2
    ORDER BY a.created_at
    LIMIT 100
  `
  static query3Params = workload =>
    new Array(workload).fill(null).map(() => {
      return [
        faker.date.between('2015-01-01', '2021-01-01'),
        faker.date.between('2015-01-01', '2021-01-01')
      ]
    })

  static query4SQL = `
    SELECT c.id, a.id
    FROM clicks as c
    JOIN ads as a
        ON c.company_id = a.company_id
               AND c.ad_id = a.id
    WHERE a.created_at > $1 AND c.cost_per_click_usd > $2
    ORDER BY c.cost_per_click_usd
    LIMIT 100
  `
  static query4Params = workload =>
    new Array(workload).fill(null).map(() => {
      return [faker.date.between('2015-01-01', '2021-01-01'), faker.datatype.number(1000) / 1000]
    })
}
module.exports = { ReadQueries }
