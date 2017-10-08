const fs = require('fs')
const ip = require('ip')
const crypto = require('crypto')
const Connection = require('../src/connection')
const { createLogger, LEVELS: { NOTHING } } = require('../src/loggers/console')

const connectionOpts = () => ({
  logger: createLogger({ level: NOTHING }),
  host: process.env.HOST_IP || ip.address(),
  port: 9092,
})

const sslConnectionOpts = () => ({
  logger: createLogger({ level: NOTHING }),
  host: process.env.HOST_IP || ip.address(),
  port: 9093,
  ssl: {
    servername: 'localhost',
    cert: fs.readFileSync('./testHelpers/certs/client_cert.pem', 'utf-8'),
    key: fs.readFileSync('./testHelpers/certs/client_key.pem', 'utf-8'),
    ca: [fs.readFileSync('./testHelpers/certs/ca_cert.pem', 'utf-8')],
  },
})

const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')
const createConnection = (opts = {}) => new Connection(Object.assign(connectionOpts(), opts))
const createModPartitioner = () => ({ partitionMetadata, message }) => {
  const numPartitions = partitionMetadata.length
  const key = parseInt(message.key.replace(/[^\d]/g, ''), 10)
  return ((key || 0) % 3) % numPartitions
}

module.exports = {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  createConnection,
  createModPartitioner,
}
