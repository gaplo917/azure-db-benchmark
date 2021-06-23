require('dotenv').config()
const { Pool } = require('pg')
const logger = require('pino')()
const numOfCopies = Number(process.argv[2]) || 1

const insertCompany = `
  INSERT INTO companies(
    id,
    name,
    image_url,
    created_at,
    updated_at
  ) VALUES (
    nextval('companies_id_seq'),
    $1,
    $2,
    $3,
    $4
  ) RETURNING id;
`
const companyToQueryParam = company => {
  return [company.name, company.image_url, company.created_at, company.updated_at]
}

const insertCampaign = `
  INSERT INTO campaigns(
    id,
    company_id,
    name,
    cost_model,
    state,
    monthly_budget,
    blacklisted_site_urls,
    created_at,
    updated_at
  ) VALUES (
    nextval('campaigns_id_seq'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8
  ) RETURNING id;
`
const campaignToQueryParam = ({ companyId, campaign }) => {
  return [
    companyId,
    campaign.name,
    campaign.cost_model,
    campaign.state,
    campaign.monthly_budget,
    campaign.blacklisted_site_urls,
    campaign.created_at,
    campaign.updated_at
  ]
}
const insertAd = `
  INSERT INTO ads(
    id,
    company_id,
    campaign_id,
    name,
    image_url,
    target_url,
    impressions_count,
    clicks_count,
    created_at,
    updated_at
  ) VALUES (
    nextval('ads_id_seq'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9
  ) RETURNING id;
`

const adToQueryParam = ({ companyId, campaignId, ad }) => {
  return [
    companyId,
    campaignId,
    ad.name,
    ad.image_url,
    ad.target_url,
    ad.impressions_count,
    ad.clicks_count,
    ad.created_at,
    ad.updated_at
  ]
}
const insertClicks = `
  INSERT INTO clicks(
    id,
    company_id,
    ad_id,
    clicked_at,
    site_url,
    cost_per_click_usd,
    user_ip,
    user_data
  ) VALUES (
    nextval('clicks_id_seq'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
  ) RETURNING id;
`
const clickToQueryParam = ({ companyId, adId, click }) => {
  return [
    companyId,
    adId,
    click.clicked_at,
    click.site_url,
    click.cost_per_click_usd,
    click.user_ip,
    click.user_data
  ]
}

const insertImpression = `
  INSERT INTO impressions(
    id,
    company_id,
    ad_id,
    seen_at,
    site_url,
    cost_per_impression_usd,
    user_ip,
    user_data
  ) VALUES (
    nextval('impressions_id_seq'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
  ) RETURNING id;
`
const impressionToQueryParam = ({ companyId, adId, impression }) => {
  return [
    companyId,
    adId,
    impression.seen_at,
    impression.site_url,
    impression.cost_per_impression_usd,
    impression.user_ip,
    impression.user_data
  ]
}

const { generateData } = require('./fake-data')
const { company, campaign, ads, click, impression, numOfRecords } = generateData(1)

const totalInsertCount = numOfRecords * numOfCopies
let inserted = 0

const busyDispatcher = async (pool, pos0) => {
  // write enough copy
  for (let writtenCopy = 0; writtenCopy < numOfCopies; writtenCopy++) {
    const { rows: r0 } = await pool.query(insertCompany, companyToQueryParam(company[pos0]))
    inserted++
    const { id: companyId } = r0[0]

    // insert campaign
    const div0 = campaign.length / company.length
    for (let i = 0; i < div0; i++) {
      const pos1 = pos0 * div0 + i
      const { rows: r1 } = await pool.query(
        insertCampaign,
        campaignToQueryParam({ companyId, campaign: campaign[pos1] })
      )
      inserted++
      const { id: campaignId } = r1[0]

      // insert ads
      const div1 = ads.length / campaign.length
      for (let j = 0; j < div1; j++) {
        const pos2 = pos1 * div1 + j
        const { rows: r2 } = await pool.query(
          insertAd,
          adToQueryParam({ companyId, campaignId, ad: ads[pos2] })
        )
        inserted++
        const { id: adId } = r2[0]

        // insert click
        const div2 = click.length / ads.length
        for (let k = 0; k < div2; k++) {
          const pos3 = pos2 * div2 + k
          await pool.query(
            insertClicks,
            clickToQueryParam({
              companyId,
              adId,
              click: click[pos3]
            })
          )
          inserted++
        }

        // insert impression
        const div3 = impression.length / ads.length
        for (let k = 0; k < div3; k++) {
          const pos3 = pos2 * div3 + k
          await pool.query(
            insertImpression,
            impressionToQueryParam({
              companyId,
              adId,
              impression: impression[pos3]
            })
          )
          inserted++
        }
      }
    }
  }
}

async function insert() {
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
    inserted,
    progress: 0,
    timeElapsedInSeconds: 0,
    numOfCopies,
    numOfRecords,
    totalInsertCount
  })

  const getTimeElapsedInSeconds = () => Number((new Date().getTime() - start) / 1000).toFixed(2)
  const getProgress = () => Number(inserted / totalInsertCount).toFixed(4)
  const getRate = () => Number(inserted / getTimeElapsedInSeconds()).toFixed(2)
  const displayProgressInterval = setInterval(() => {
    logger.info({
      inserted,
      progress: getProgress(),
      insertRate: `${getRate()}/s`,
      timeElapsedInSeconds: getTimeElapsedInSeconds()
    })
  }, 1000)

  await Promise.all(
    new Array(concurrency)
      .fill(null)
      .map((_, index) => busyDispatcher(pool, index % company.length))
  )

  clearInterval(displayProgressInterval)

  logger.info({
    inserted,
    progress: getProgress(),
    insertRate: `${getRate()}/s`,
    timeElapsedInSeconds: getTimeElapsedInSeconds()
  })

  // release pool before exist
  pool.end()

  process.exit(0)
}

insert()
