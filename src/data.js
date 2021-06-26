const path = require('path')
const fsPromises = require('fs').promises
const { argv } = require('yargs/yargs')(process.argv.slice(2))
const { generateData } = require('./generate-data')

// write to file
if (argv.write) {
  // JSON.stringify is not able to parse a large file in V8, that's why we need to spread it to write item-by-item.
  const writeToFile = async (file, arr) => {
    await fsPromises.unlink(file)
    await fsPromises.appendFile(file, '[')
    for (let i = 0; i < arr.length; i++) {
      await fsPromises.appendFile(file, JSON.stringify(arr[i], null) + ',')
    }
    await fsPromises.appendFile(file, ']')
  }

  ;(async function main() {
    const dataFolder = argv.seed
    const seedDir = path.resolve(__dirname + '/../data/' + dataFolder)
    const isExist = await fsPromises.exists(seedDir)
    if (!isExist) {
      await fsPromises.mkdir(seedDir)
    }
    const { company, campaign, ads, click, impression } = generateData(argv.seed)

    writeToFile(seedDir + '/company.json', company, null)
    writeToFile(seedDir + '/campaign.json', campaign, null)
    writeToFile(seedDir + '/ads.json', ads, null)
    writeToFile(seedDir + '/click.json', click, null)
    writeToFile(seedDir + '/impression.json', impression, null)
  })()
}
