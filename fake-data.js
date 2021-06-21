const faker = require('faker')
const path = require('path')
const fs = require('fs')
const companyCount = 2000
const campaignCount = companyCount * 10
const adsCount = campaignCount
const clickCount = adsCount * 10
const impressionCount = clickCount * 10

function generateData(dir, seed) {
  // reproducible generation
  faker.seed(seed)

  const company = new Array(companyCount).fill(null).map(() => ({
    name: faker.name.findName(faker.name.firstName(), faker.name.lastName(), faker.name.gender()),
    image_url: faker.image.imageUrl(faker.datatype.number(), faker.datatype.number()),
    created_at: faker.date.between('2015-01-01', '2021-01-01'),
    updated_at: faker.date.between('2021-01-01', '2021-06-01')
  }))

  fs.writeFileSync(dir + '/company.json', JSON.stringify(company, null))

  const campaign = new Array(campaignCount).fill(null).map(() => ({
    name: faker.name.findName(faker.name.firstName(), faker.name.lastName(), faker.name.gender()),
    cost_model: faker.image.imageUrl(faker.datatype.number(), faker.datatype.number()),
    state: faker.address.state(),
    monthly_budget: faker.datatype.number(),
    blacklisted_site_urls: [faker.internet.url()],
    created_at: faker.date.between('2015-01-01', '2021-01-01'),
    updated_at: faker.date.between('2021-01-01', '2021-06-01')
  }))

  fs.writeFileSync(dir + '/campaign.json', JSON.stringify(campaign, null))

  const ads = new Array(adsCount).fill(null).map(() => ({
    name: faker.name.findName(faker.name.firstName(), faker.name.lastName(), faker.name.gender()),
    image_url: faker.image.imageUrl(faker.datatype.number(), faker.datatype.number()),
    target_url: faker.internet.url(),
    impressions_count: faker.datatype.number(),
    clicks_count: faker.datatype.number(),
    created_at: faker.date.between('2015-01-01', '2021-01-01'),
    updated_at: faker.date.between('2021-01-01', '2021-06-01')
  }))

  fs.writeFileSync(dir + '/ads.json', JSON.stringify(ads, null))

  const click = new Array(clickCount).fill(null).map(() => ({
    clicked_at: faker.date.between('2015-01-01', '2021-01-01'),
    site_url: faker.internet.url(),
    cost_per_click_usd: faker.datatype.number(1000) / 1000,
    user_ip: faker.internet.ip(),
    user_data: faker.datatype.json()
  }))

  fs.writeFileSync(dir + '/click.json', JSON.stringify(click, null))

  const impression = new Array(impressionCount).fill(null).map(() => ({
    seen_at: faker.date.between('2015-01-01', '2021-01-01'),
    site_url: faker.internet.url(),
    cost_per_impression_usd: faker.datatype.number(1000) / 10000,
    user_ip: faker.internet.ip(),
    user_data: faker.datatype.json()
  }))

  fs.writeFileSync(dir + '/impression.json', JSON.stringify(impression, null))
}
const [set1, set2] = [path.resolve('./data/set1'), path.resolve('./data/set2')]

generateData(set1, 1)
generateData(set2, 2)
