const { Worker, isMainThread, parentPort } = require('worker_threads')
const argv = require('yargs/yargs')(process.argv.slice(2)).argv

class Message {
  static INIT = 'INIT'
  static PROGRESS = 'PROGRESS'
  static createInitMessage(payload) {
    return new Message({ type: Message.INIT, payload })
  }
  static createProgressMessage(payload) {
    return new Message({ type: Message.PROGRESS, payload })
  }
  constructor({ type, payload }) {
    this.type = type
    this.payload = payload
  }
}

if (isMainThread) {
  function test() {
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename)
      worker.on('message', resolve)
      worker.on('error', reject)
      worker.on('exit', code => {
        if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`))
      })
    })
  }
  test().then(console.log)
} else {
  parentPort.postMessage(Message.createInitMessage({ abc: 123, def: 345, argv }))
}

console.log(argv)
