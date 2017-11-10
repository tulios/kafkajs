const fs = require('fs')
const ip = require('ip')
const crypto = require('crypto')
const Cluster = require('../src/cluster')
const Connection = require('../src/network/connection')
const { createLogger, LEVELS: { NOTHING } } = require('../src/loggers')
const logFunctionConsole = require('../src/loggers/console')

const newLogger = () => createLogger({ level: NOTHING, logFunction: logFunctionConsole })
const getHost = () => process.env.HOST_IP || ip.address()
const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')
const plainTextBrokers = (host = getHost()) => [`${host}:9092`, `${host}:9095`, `${host}:9098`]
const sslBrokers = (host = getHost()) => [`${host}:9093`, `${host}:9096`, `${host}:9099`]
const saslBrokers = (host = getHost()) => [`${host}:9094`, `${host}:9097`, `${host}:9100`]

const connectionOpts = () => ({
  clientId: `test-${secureRandom()}`,
  logger: newLogger(),
  host: getHost(),
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
const createCluster = (opts = {}, brokers = plainTextBrokers()) =>
  new Cluster(Object.assign(connectionOpts(), opts, { brokers }))

const createModPartitioner = () => ({ partitionMetadata, message }) => {
  const numPartitions = partitionMetadata.length
  const key = parseInt(message.key.replace(/[^\d]/g, ''), 10)
  return ((key || 0) % 3) % numPartitions
}

const retryProtocol = (errorType, fn) =>
  new Promise((resolve, reject) => {
    const schedule = () =>
      setTimeout(async () => {
        try {
          const result = await fn()
          resolve(result)
        } catch (e) {
          if (e.type !== errorType) {
            return reject(e)
          }
          schedule()
        }
      }, 100)

    schedule()
  })

const createTopic = (broker, topicName) =>
  retryProtocol('LEADER_NOT_AVAILABLE', async () => await broker.metadata([topicName]))

module.exports = {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  saslConnectionOpts,
  createConnection,
  createCluster,
  createModPartitioner,
  plainTextBrokers,
  sslBrokers,
  saslBrokers,
  newLogger,
  retryProtocol,
  createTopic,
}
