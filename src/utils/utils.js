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

const divideWorkFairly = (target, n) => {
  const a = Math.ceil(target / n)
  const b = Math.floor(target / n)
  let count = 0
  const arr = []
  for (let i = 0; i < n - 1; i++) {
    if (i % 2 === 0) {
      count += a
      arr.push(a)
    } else {
      count += b
      arr.push(b)
    }
  }
  arr.push(target - count)
  return arr
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

const timeElapsedInSecondsSince = start => Number((new Date().getTime() - start) / 1000).toFixed(2)

module.exports = { Message, divideWorkFairly, delay, timeElapsedInSecondsSince }
