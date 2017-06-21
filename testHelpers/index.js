const ip = require('ip')
const crypto = require('crypto')
const Connection = require('../src/connection')
const { createLogger, LEVELS: { NOTHING } } = require('../src/loggers/console')

const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')
const createConnection = (opts = {}) =>
  new Connection(
    Object.assign(
      {
        host: process.env.HOST_IP || ip.address(),
        port: 9092,
        logger: createLogger({ level: NOTHING }),
      },
      opts
    )
  )

module.exports = {
  secureRandom,
  createConnection,
}
