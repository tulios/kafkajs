const fs = require('fs')
const ip = require('ip')
const execa = require('execa')
const uuid = require('uuid/v4')
const crypto = require('crypto')
const Cluster = require('../src/cluster')
const waitFor = require('../src/utils/waitFor')
const connectionBuilder = require('../src/cluster/connectionBuilder')
const Connection = require('../src/network/connection')

const {
  createLogger,
  LEVELS: { NOTHING, INFO, DEBUG },
} = require('../src/loggers')

const LoggerConsole = require('../src/loggers/console')
const Kafka = require('../src/index')

const isTravis = process.env.TRAVIS === 'true'
const travisLevel = process.env.VERBOSE ? DEBUG : INFO

const newLogger = (opts = {}) =>
  createLogger(
    Object.assign({ level: isTravis ? travisLevel : NOTHING, logCreator: LoggerConsole }, opts)
  )

const getHost = () => process.env.HOST_IP || ip.address()
const secureRandom = (length = 10) =>
  `${crypto.randomBytes(length).toString('hex')}-${process.pid}-${uuid()}`

const plainTextBrokers = (host = getHost()) => [`${host}:9092`, `${host}:9095`, `${host}:9098`]
const sslBrokers = (host = getHost()) => [`${host}:9093`, `${host}:9096`, `${host}:9099`]
const saslBrokers = (host = getHost()) => [`${host}:9094`, `${host}:9097`, `${host}:9100`]

const connectionOpts = (opts = {}) => ({
  clientId: `test-${secureRandom()}`,
  connectionTimeout: 3000,
  logger: newLogger(),
  host: getHost(),
  port: 9092,
  ...opts,
})

const sslConnectionOpts = () =>
  Object.assign(connectionOpts(), {
    port: 9093,
    ssl: {
      servername: 'localhost',
      rejectUnauthorized: false,
      ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
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

const saslSCRAM256ConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-256',
      username: 'testscram',
      password: 'testtestscram256',
    },
  })

const saslSCRAM512ConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-512',
      username: 'testscram',
      password: 'testtestscram512',
    },
  })

const createConnection = (opts = {}) => new Connection(Object.assign(connectionOpts(), opts))

const createConnectionBuilder = (opts = {}, brokers = plainTextBrokers()) => {
  const { ssl, sasl, clientId } = Object.assign(connectionOpts(), opts)
  return connectionBuilder({
    logger: newLogger(),
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout: 1000,
    retry: null,
  })
}

const createCluster = (opts = {}, brokers = plainTextBrokers()) =>
  new Cluster(Object.assign(connectionOpts(), opts, { brokers }))

const createModPartitioner = () => ({ partitionMetadata, message }) => {
  const numPartitions = partitionMetadata.length
  const key = parseInt(message.key.replace(/[^\d]/g, ''), 10)
  return ((key || 0) % 3) % numPartitions
}

const testWaitFor = async (fn, opts = {}) => waitFor(fn, { ...opts, ignoreTimeout: true })

const retryProtocol = (errorType, fn) =>
  waitFor(
    async () => {
      try {
        return await fn()
      } catch (e) {
        if (e.type !== errorType) {
          throw e
        }
        return false
      }
    },
    { ignoreTimeout: true }
  )

const waitForMessages = (buffer, { number = 1, delay = 50 } = {}) =>
  waitFor(() => (buffer.length >= number ? buffer : false), { delay, ignoreTimeout: true })

const waitForConsumerToJoinGroup = (consumer, { maxWait = 10000 } = {}) =>
  new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => reject(new Error('Timeout')), maxWait)
    consumer.on(consumer.events.GROUP_JOIN, () => {
      clearTimeout(timeoutId)
      resolve()
    })
  })

const createTopic = async ({ topic, partitions = 1, config = [] }) => {
  const kafka = new Kafka({ clientId: 'testHelpers', brokers: [`${getHost()}:9092`] })
  const admin = kafka.admin()

  try {
    await admin.connect()
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic, numPartitions: partitions, configEntries: config }],
    })
  } finally {
    admin && (await admin.disconnect())
  }
}

const addPartitions = async ({ topic, partitions }) => {
  const cmd = `TOPIC=${topic} PARTITIONS=${partitions} ./scripts/addPartitions.sh`
  const cluster = createCluster()

  await cluster.connect()
  await cluster.addTargetTopic(topic)

  execa.shellSync(cmd)

  waitFor(async () => {
    await cluster.refreshMetadata()
    const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
    return partitionMetadata.length === partitions
  })
}

const testIfKafka011 = (description, callback) => {
  return process.env.KAFKA_VERSION === '0.11'
    ? test(description, callback)
    : test.skip(description, callback)
}

const unsupportedVersionResponse = () => Buffer.from({ type: 'Buffer', data: [0, 35, 0, 0, 0, 0] })

module.exports = {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  saslConnectionOpts,
  saslSCRAM256ConnectionOpts,
  saslSCRAM512ConnectionOpts,
  createConnection,
  createConnectionBuilder,
  createCluster,
  createModPartitioner,
  plainTextBrokers,
  sslBrokers,
  saslBrokers,
  newLogger,
  retryProtocol,
  createTopic,
  waitFor: testWaitFor,
  waitForMessages,
  waitForConsumerToJoinGroup,
  testIfKafka011,
  addPartitions,
  unsupportedVersionResponse,
}
