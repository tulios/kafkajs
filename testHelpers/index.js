const ip = require('ip')
const crypto = require('crypto')
const Connection = require('../src/connection')
const { createLogger, LEVELS: { NOTHING } } = require('../src/loggers/console')

const connectionOpts = () => ({
  host: process.env.HOST_IP || ip.address(),
  port: 9092,
  logger: createLogger({ level: NOTHING }),
})
const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')
const createConnection = (opts = {}) => new Connection(Object.assign(connectionOpts(), opts))
const createModPartitioner = () => ({ partitionMetadata, message }) => {
  const numPartitions = partitionMetadata.length
  const key = parseInt(message.key.replace(/[^\d]/g, ''), 10)
  return (key || 0) % 3 % numPartitions
}

module.exports = {
  secureRandom,
  connectionOpts,
  createConnection,
  createModPartitioner,
}
