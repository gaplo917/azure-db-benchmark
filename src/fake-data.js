const faker = require('faker')
const path = require('path')
const fs = require('fs')
const companyCount = 2000
const campaignCount = companyCount * 10
const adsCount = campaignCount
const clickCount = adsCount * 10
const impressionCount = clickCount * 10
const logger = require('pino')()
const { argv } = require('yargs/yargs')(process.argv.slice(2))

function generateData(seed) {
  logger.info({
    message: 'generating data',
    seed
  })
  // reproducible generation
  faker.seed(seed)

  const company = new Array(companyCount).fill(null).map(() => ({
    name: faker.name.findName(faker.name.firstName(), faker.name.lastName(), faker.name.gender()),
    image_url: faker.image.imageUrl(faker.datatype.number(), faker.datatype.number()),
    created_at: faker.date.between('2015-01-01', '2021-01-01'),
    updated_at: faker.date.between('2021-01-01', '2021-06-01')
  }))

  const campaign = new Array(campaignCount).fill(null).map(() => ({
    name: faker.name.findName(faker.name.firstName(), faker.name.lastName(), faker.name.gender()),
    cost_model: faker.image.imageUrl(faker.datatype.number(), faker.datatype.number()),
    state: faker.address.state(),
    monthly_budget: faker.datatype.number(),
    blacklisted_site_urls: [faker.internet.url()],
    created_at: faker.date.between('2015-01-01', '2021-01-01'),
    updated_at: faker.date.between('2021-01-01', '2021-06-01')
  }))

  const ads = new Array(adsCount).fill(null).map(() => ({
    name: faker.name.findName(faker.name.firstName(), faker.name.lastName(), faker.name.gender()),
    image_url: faker.image.imageUrl(faker.datatype.number(), faker.datatype.number()),
    target_url: faker.internet.url(),
    impressions_count: faker.datatype.number(),
    clicks_count: faker.datatype.number(),
    created_at: faker.date.between('2015-01-01', '2021-01-01'),
    updated_at: faker.date.between('2021-01-01', '2021-06-01')
  }))

  const click = new Array(clickCount).fill(null).map(() => ({
    clicked_at: faker.date.between('2015-01-01', '2021-01-01'),
    site_url: faker.internet.url(),
    cost_per_click_usd: faker.datatype.number(1000) / 1000,
    user_ip: faker.internet.ip(),
    user_data: faker.datatype.json()
  }))

  const impression = new Array(impressionCount).fill(null).map(() => ({
    seen_at: faker.date.between('2015-01-01', '2021-01-01'),
    site_url: faker.internet.url(),
    cost_per_impression_usd: faker.datatype.number(1000) / 10000,
    user_ip: faker.internet.ip(),
    user_data: faker.datatype.json()
  }))

  const numOfRecords =
    company.length + campaign.length + ads.length + click.length + impression.length

  return {
    company,
    campaign,
    ads,
    click,
    impression,
    numOfRecords
  }
}

// write to file
if (argv.write) {
  const dataFolder = argv.seed
  const seedDir = path.resolve(__dirname + '/../data/' + dataFolder)
  if (!fs.existsSync(seedDir)) {
    fs.mkdirSync(seedDir)
  }
  const { company, campaign, ads, click, impression } = generateData(argv.seed)

  fs.writeFileSync(seedDir + '/company.json', JSON.stringify(company, null))
  fs.writeFileSync(seedDir + '/campaign.json', JSON.stringify(campaign, null))
  fs.writeFileSync(seedDir + '/ads.json', JSON.stringify(ads, null))
  fs.writeFileSync(seedDir + '/click.json', JSON.stringify(click, null))
  fs.writeFileSync(seedDir + '/impression.json', JSON.stringify(impression, null))
}

module.exports = { generateData }
