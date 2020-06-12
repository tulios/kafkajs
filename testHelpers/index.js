const fs = require('fs')
const execa = require('execa')
const uuid = require('uuid/v4')
const semver = require('semver')
const crypto = require('crypto')
const Cluster = require('../src/cluster')
const waitFor = require('../src/utils/waitFor')
const connectionBuilder = require('../src/cluster/connectionBuilder')
const Connection = require('../src/network/connection')
const defaultSocketFactory = require('../src/network/socketFactory')
const socketFactory = defaultSocketFactory()

const {
  createLogger,
  LEVELS: { NOTHING },
} = require('../src/loggers')

const LoggerConsole = require('../src/loggers/console')
const { Kafka } = require('../index')

const newLogger = (opts = {}) =>
  createLogger(Object.assign({ level: NOTHING, logCreator: LoggerConsole }, opts))

const getHost = () => 'localhost'
const secureRandom = (length = 10) =>
  `${crypto.randomBytes(length).toString('hex')}-${process.pid}-${uuid()}`

const plainTextBrokers = (host = getHost()) => [`${host}:9092`, `${host}:9095`, `${host}:9098`]
const sslBrokers = (host = getHost()) => [`${host}:9093`, `${host}:9096`, `${host}:9099`]
const saslBrokers = (host = getHost()) => [`${host}:9094`, `${host}:9097`, `${host}:9100`]

const connectionOpts = (opts = {}) => ({
  socketFactory,
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
      password: 'testtestscram=256',
    },
  })

const saslSCRAM512ConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-512',
      username: 'testscram',
      password: 'testtestscram=512',
    },
  })

const createConnection = (opts = {}) => new Connection(Object.assign(connectionOpts(), opts))

const createConnectionBuilder = (opts = {}, brokers = plainTextBrokers()) => {
  const { ssl, sasl, clientId } = Object.assign(connectionOpts(), opts)
  return connectionBuilder({
    socketFactory,
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

const testWaitFor = async (fn, opts = {}) => waitFor(fn, { ignoreTimeout: true, ...opts })

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

const waitForNextEvent = (consumer, eventName, { maxWait = 10000 } = {}) =>
  new Promise((resolve, reject) => {
    const timeoutId = setTimeout(
      () => reject(new Error(`Timeout waiting for '${eventName}'`)),
      maxWait
    )
    consumer.on(eventName, event => {
      clearTimeout(timeoutId)
      resolve(event)
    })
    consumer.on(consumer.events.CRASH, event => {
      clearTimeout(timeoutId)
      reject(event.payload.error)
    })
  })

const waitForConsumerToJoinGroup = (consumer, { maxWait = 10000, label = '' } = {}) =>
  new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      consumer.disconnect().then(() => {
        reject(new Error(`Timeout ${label}`.trim()))
      })
    }, maxWait)
    consumer.on(consumer.events.GROUP_JOIN, event => {
      clearTimeout(timeoutId)
      resolve(event)
    })
    consumer.on(consumer.events.CRASH, event => {
      clearTimeout(timeoutId)
      consumer.disconnect().then(() => {
        reject(event.payload.error)
      })
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

  execa.commandSync(cmd, { shell: true })

  waitFor(async () => {
    await cluster.refreshMetadata()
    const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
    return partitionMetadata.length === partitions
  })
}

const testIfKafkaVersion = version => (description, callback, testFn = test) => {
  return semver.gte(semver.coerce(process.env.KAFKA_VERSION), semver.coerce(version))
    ? testFn(description, callback)
    : test.skip(description, callback)
}

const testIfKafka_0_11 = testIfKafkaVersion('0.11')
testIfKafka_0_11.only = (description, callback) => {
  return testIfKafka_0_11(description, callback, test.only)
}

const testIfKafka_1_1_0 = testIfKafkaVersion('1.1')
testIfKafka_1_1_0.only = (description, callback) => {
  return testIfKafka_1_1_0(description, callback, test.only)
}

const unsupportedVersionResponse = () => Buffer.from({ type: 'Buffer', data: [0, 35, 0, 0, 0, 0] })
const unsupportedVersionResponseWithTimeout = () =>
  Buffer.from({ type: 'Buffer', data: [0, 0, 0, 0, 0, 35] })

const generateMessages = options => {
  const { prefix, number = 100 } = options || {}
  const prefixOrEmpty = prefix ? `-${prefix}` : ''

  return Array(number)
    .fill()
    .map((v, i) => {
      const value = secureRandom()
      return {
        key: `key${prefixOrEmpty}-${i}-${value}`,
        value: `value${prefixOrEmpty}-${i}-${value}`,
      }
    })
}

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
  waitForNextEvent,
  waitForConsumerToJoinGroup,
  testIfKafka_0_11,
  testIfKafka_1_1_0,
  addPartitions,
  unsupportedVersionResponse,
  generateMessages,
  unsupportedVersionResponseWithTimeout,
}
