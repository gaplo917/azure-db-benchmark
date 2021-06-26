require('dotenv').config()
const logger = require('pino')()
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads')
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { worker: workerCount = 1, concurrency = 2000, maxDbConnection = 50, numOfCopies = 1 } = argv

class Message {
  static INIT = 'INIT'
  static PROGRESS = 'PROGRESS'
  static DONE = 'DONE'

  static createInitMessage(payload) {
    return { type: Message.INIT, payload }
  }

  static createProgressMessage(payload) {
    return { type: Message.PROGRESS, payload }
  }

  static createDoneMessage(payload) {
    return { type: Message.DONE, payload }
  }
}

const divideIntegerFairly = (target, n) => {
  const a = Math.floor(target / n)
  return [...new Array(n - 1).fill(a), target - a * (n - 1)]
}

if (isMainThread) {
  const workerStats = new Map()
  function createWorker({ workerId, concurrency, maxDbConnection }) {
    logger.info({
      message: 'create new worker',
      worker: { workerId, concurrency, maxDbConnection }
    })
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename, {
        workerData: {
          concurrency: concurrency,
          maxDbConnection: maxDbConnection,
          numOfCopies
        }
      })
      worker.on('message', ({ type, payload }) => {
        switch (type) {
          case Message.INIT:
            workerStats.set(workerId, {
              isDone: false,
              totalInsertCount: Number(payload.totalInsertCount),
              inserted: 0,
              timeElapsedInSeconds: 0
            })
            logger.info({
              message: 'new worker join',
              workerPayload: payload,
              totalInsertCount: Array.from(workerStats.values())
                .map(it => it.totalInsertCount)
                .reduce((acc, e) => acc + e, 0)
            })
            break
          case Message.PROGRESS:
            workerStats.set(workerId, {
              ...workerStats.get(workerId),
              timeout: Number(payload.timeout),
              inserted: Number(payload.inserted),
              timeElapsedInSeconds: Number(payload.timeElapsedInSeconds)
            })
            break
          case Message.DONE:
            workerStats.set(workerId, {
              ...workerStats.get(workerId),
              isDone: true,
              timeout: Number(payload.timeout),
              inserted: Number(payload.inserted),
              timeElapsedInSeconds: Number(payload.timeElapsedInSeconds)
            })
            logger.info({
              message: 'worker done',
              workerId,
              workerPayload: payload
            })
            break
        }
      })
      worker.on('error', reject)
      worker.on('exit', code => {
        if (code !== 0) {
          logger.error(new Error(`Worker stopped with exit code ${code}`))
        }
        resolve()
      })
    })
  }

  async function reporter() {
    let start = new Date().getTime()

    const getTimeElapsedInSeconds = () => Number((new Date().getTime() - start) / 1000).toFixed(2)
    const aggregateInserted = stats => {
      return stats.map(it => it.inserted).reduce((acc, e) => acc + e, 0)
    }
    const aggregateTotalInsertCount = stats => {
      return stats.map(it => it.totalInsertCount).reduce((acc, e) => acc + e, 0)
    }
    const aggregateTotalTimeout = stats => {
      return stats.map(it => it.timeout).reduce((acc, e) => acc + e, 0)
    }
    const aggregateTimeUsed = stats => {
      return stats.map(it => it.timeElapsedInSeconds).reduce((acc, e) => acc + e, 0)
    }
    const calcProgress = stats => {
      return Number(aggregateInserted(stats) / aggregateTotalInsertCount(stats)).toFixed(4)
    }
    const calcAvgRate = stats => {
      const avgTimeUsed = aggregateTimeUsed(stats) / stats.length
      return Number(aggregateInserted(stats) / avgTimeUsed).toFixed(2)
    }

    const startScheduledReport = () => {
      let lastRecord = {
        totalInserted: 0,
        timeElapsedInSeconds: 0
      }
      const interval = setInterval(() => {
        if (workerStats.size === 0) {
          return
        }
        const stats = Array.from(workerStats.values())

        const data = {
          totalInserted: aggregateInserted(stats),
          totalTimeout: aggregateTotalTimeout(stats),
          totalTimeUsed: aggregateTimeUsed(stats),
          progress: calcProgress(stats),
          avgInsertRate: `${calcAvgRate(stats)}/s`,
          timeElapsedInSeconds: getTimeElapsedInSeconds()
        }
        const insertedDiff = data.totalInserted - lastRecord.totalInserted
        const timeElapsedDiff = data.timeElapsedInSeconds - lastRecord.timeElapsedInSeconds

        logger.info({
          totalInserted: aggregateInserted(stats),
          totalTimeout: aggregateTotalTimeout(stats),
          totalTimeUsed: aggregateTimeUsed(stats),
          progress: calcProgress(stats),
          currentInsertRate: Number(insertedDiff / timeElapsedDiff).toFixed(2),
          avgInsertRate: `${calcAvgRate(stats)}/s`,
          timeElapsedInSeconds: getTimeElapsedInSeconds()
        })
        lastRecord = {
          totalInserted: data.totalInserted,
          timeElapsedInSeconds: data.timeElapsedInSeconds
        }
      }, 1000)

      return () => {
        clearInterval(interval)
      }
    }

    const stopScheduledReport = startScheduledReport()

    const concurrencyArr = divideIntegerFairly(concurrency, workerCount)
    const maxDBConnectionArr = divideIntegerFairly(maxDbConnection, workerCount)
    await Promise.all(
      new Array(workerCount).fill(null).map((_, index) =>
        createWorker({
          workerId: index,
          concurrency: concurrencyArr[index],
          maxDbConnection: maxDBConnectionArr[index]
        })
      )
    )

    stopScheduledReport()

    const stats = Array.from(workerStats.values())
    logger.info({
      totalInserted: aggregateInserted(stats),
      totalTimeout: aggregateTotalTimeout(stats),
      totalTimeUsed: aggregateTimeUsed(stats),
      progress: calcProgress(stats),
      avgInsertRate: `${calcAvgRate(stats)}/s`,
      timeElapsedInSeconds: getTimeElapsedInSeconds()
    })
  }

  // start
  reporter()
} else {
  const { Pool } = require('pg')
  const { generateData } = require('./fake-data')

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

  let inserted = 0
  let timeout = 0

  const timeoutHandler = () => timeout++

  const busyDispatcher = async ({ pool, index, numOfCopies, data }) => {
    const { company, campaign, ads, click, impression } = data
    const pos0 = index % company.length
    // write enough copy
    for (let writtenCopy = 0; writtenCopy < numOfCopies; writtenCopy++) {
      const { rows: r0 } = await pool.query(insertCompany, companyToQueryParam(company[pos0]))
      inserted++
      const { id: companyId } = r0[0]

      // insert campaign
      const div0 = campaign.length / company.length
      for (let i = 0; i < div0; i++) {
        const pos1 = pos0 * div0 + i
        const { rows: r1 } = await pool
          .query(insertCampaign, campaignToQueryParam({ companyId, campaign: campaign[pos1] }))
          .catch(timeoutHandler)
        inserted++
        const { id: campaignId } = r1[0]

        // insert ads
        const div1 = ads.length / campaign.length
        for (let j = 0; j < div1; j++) {
          const pos2 = pos1 * div1 + j
          const { rows: r2 } = await pool
            .query(insertAd, adToQueryParam({ companyId, campaignId, ad: ads[pos2] }))
            .catch(timeoutHandler)
          inserted++
          const { id: adId } = r2[0]

          // insert click
          const div2 = click.length / ads.length
          for (let k = 0; k < div2; k++) {
            const pos3 = pos2 * div2 + k
            await pool
              .query(
                insertClicks,
                clickToQueryParam({
                  companyId,
                  adId,
                  click: click[pos3]
                })
              )
              .catch(timeoutHandler)
            inserted++
          }

          // insert impression
          const div3 = impression.length / ads.length
          for (let k = 0; k < div3; k++) {
            const pos3 = pos2 * div3 + k
            await pool
              .query(
                insertImpression,
                impressionToQueryParam({
                  companyId,
                  adId,
                  impression: impression[pos3]
                })
              )
              .catch(timeoutHandler)
            inserted++
          }
        }
      }
    }
  }

  async function insert() {
    const { concurrency = 2000, maxDbConnection = 50, numOfCopies = 1 } = workerData
    const pool = new Pool({
      connectionString: process.env.PGCONNECTIONSTRING,
      max: maxDbConnection,
      idleTimeoutMillis: 30 * 1000,
      connectionTimeoutMillis: 60 * 1000,
      query_timeout: 5 * 60 * 1000
    })
    await pool.connect()

    // generate all dummy data in memory first
    const data = generateData(1)

    const start = new Date().getTime()

    parentPort.postMessage(
      Message.createInitMessage({
        totalInsertCount: numOfCopies * data.numOfRecords,
        concurrency,
        maxDbConnection
      })
    )

    const getTimeElapsedInSeconds = () => Number((new Date().getTime() - start) / 1000).toFixed(2)

    const reportProgressInterval = setInterval(() => {
      parentPort.postMessage(
        Message.createProgressMessage({
          inserted,
          timeout,
          timeElapsedInSeconds: getTimeElapsedInSeconds()
        })
      )
    }, 1000)

    await Promise.all(
      new Array(concurrency)
        .fill(null)
        .map((_, index) => busyDispatcher({ pool, index, numOfCopies, data }))
    )

    clearInterval(reportProgressInterval)

    // report the last status
    parentPort.postMessage(
      Message.createDoneMessage({
        inserted,
        timeout,
        timeElapsedInSeconds: getTimeElapsedInSeconds()
      })
    )

    // release pool before exit
    pool.end()
  }

  insert()
}
