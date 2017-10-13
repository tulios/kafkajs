const fs = require('fs')
const ip = require('ip')
const crypto = require('crypto')
const Cluster = require('../src/cluster')
const Connection = require('../src/network/connection')
const { createLogger, LEVELS: { NOTHING } } = require('../src/loggers/console')

const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')

const connectionOpts = () => ({
  clientId: `test-${secureRandom()}`,
  logger: createLogger({ level: NOTHING }),
  host: process.env.HOST_IP || ip.address(),
  port: 9092,
})

const sslConnectionOpts = () =>
  Object.assign(connectionOpts(), {
    port: 9093,
    ssl: {
      servername: 'localhost',
      cert: fs.readFileSync('./testHelpers/certs/client_cert.pem', 'utf-8'),
      key: fs.readFileSync('./testHelpers/certs/client_key.pem', 'utf-8'),
      ca: [fs.readFileSync('./testHelpers/certs/ca_cert.pem', 'utf-8')],
    },
  })

const saslConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'plain',
      username: 'test',
      password: 'testtest',
    },
  })

const createConnection = (opts = {}) => new Connection(Object.assign(connectionOpts(), opts))
const createCluster = (opts = {}) => new Cluster(Object.assign(connectionOpts(), opts))
const createModPartitioner = () => ({ partitionMetadata, message }) => {
  const numPartitions = partitionMetadata.length
  const key = parseInt(message.key.replace(/[^\d]/g, ''), 10)
  return ((key || 0) % 3) % numPartitions
}

module.exports = {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  saslConnectionOpts,
  createConnection,
  createCluster,
  createModPartitioner,
}
