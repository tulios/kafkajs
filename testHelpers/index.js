/* eslint-disable jest/valid-describe-callback */
// Disabled to allow for higher order test functions where
// `callback` is parameterized instead of a static function
const fs = require('fs')
const execa = require('execa')
const uuid = require('uuid/v4')
const semver = require('semver')
const crypto = require('crypto')
const jwt = require('jsonwebtoken')
const Cluster = require('../src/cluster')
const waitFor = require('../src/utils/waitFor')
const connectionBuilder = require('../src/cluster/connectionPoolBuilder')
const ConnectionPool = require('../src/network/connectionPool')
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
  requestTimeout: 30000,
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

const saslWrongConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'plain',
      username: 'wrong',
      password: 'wrong',
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

const saslSCRAM256WrongConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-256',
      username: 'wrong',
      password: 'wrong',
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

const saslSCRAM512WrongConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-512',
      username: 'wrong',
      password: 'wrong',
    },
  })

const saslOAuthBearerConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: () => {
        const token = jwt.sign({ sub: 'test' }, undefined, { algorithm: 'none', expiresIn: '1h' })

        return {
          value: token,
        }
      },
    },
  })

/**
 * List of the possible SASL setups.
 * OAUTHBEARER must be enabled as a special case.
 */
const saslEntries = []
if (process.env['OAUTHBEARER_ENABLED'] !== '1') {
  saslEntries.push({
    name: 'PLAIN',
    opts: saslConnectionOpts,
    wrongOpts: saslWrongConnectionOpts,
    expectedErr: /SASL PLAIN authentication failed/,
  })

  saslEntries.push({
    name: 'SCRAM 256',
    opts: saslSCRAM256ConnectionOpts,
    wrongOpts: saslSCRAM256WrongConnectionOpts,
    expectedErr: /SASL SCRAM SHA256 authentication failed/,
  })

  saslEntries.push({
    name: 'SCRAM 512',
    opts: saslSCRAM512ConnectionOpts,
    wrongOpts: saslSCRAM512WrongConnectionOpts,
    expectedErr: /SASL SCRAM SHA512 authentication failed/,
  })
} else {
  saslEntries.push({
    name: 'OAUTHBEARER',
    opts: saslOAuthBearerConnectionOpts,
  })
}

const createConnectionPool = (opts = {}) =>
  new ConnectionPool(Object.assign(connectionOpts(), opts))

const createConnectionBuilder = (opts = {}, brokers = plainTextBrokers()) => {
  return connectionBuilder({
    socketFactory,
    logger: newLogger(),
    brokers,
    connectionTimeout: 1000,
    ...connectionOpts(),
    ...opts,
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

/**
 * @param {import("../types").KafkaJSError} errorType
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 * @template T
 */
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

const createTopic = async ({ topic, partitions = 1, replicas = 1, config = [] }) => {
  const kafka = new Kafka({ clientId: 'testHelpers', brokers: [`${getHost()}:9092`] })
  const admin = kafka.admin()

  try {
    await admin.connect()
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        { topic, numPartitions: partitions, replicationFactor: replicas, configEntries: config },
      ],
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

const testIfKafkaVersion = (version, versionComparator) => {
  const scopedTest = (description, callback, testFn = test) => {
    return versionComparator(semver.coerce(process.env.KAFKA_VERSION), semver.coerce(version))
      ? testFn(description, callback)
      : test.skip(description, callback)
  }

  scopedTest.only = (description, callback) => scopedTest(description, callback, test.only)

  return scopedTest
}

const testIfKafkaVersionLTE = version => testIfKafkaVersion(version, semver.lte)
const testIfKafkaVersionGTE = version => testIfKafkaVersion(version, semver.gte)

const testIfKafkaAtMost_0_10 = testIfKafkaVersionLTE('0.10')
const testIfKafkaAtLeast_0_11 = testIfKafkaVersionGTE('0.11')
const testIfKafkaAtLeast_1_1_0 = testIfKafkaVersionGTE('1.1')

const flakyTest = (description, callback, testFn = test) =>
  testFn(`[flaky] ${description}`, callback)
flakyTest.skip = (description, callback) => flakyTest(description, callback, test.skip)
flakyTest.only = (description, callback) => flakyTest(description, callback, test.only)
const describeIfEnv = (key, value) => (description, callback, describeFn = describe) => {
  return value === process.env[key]
    ? describeFn(description, callback)
    : describe.skip(description, callback)
}

const describeIfNotEnv = (key, value) => (description, callback, describeFn = describe) => {
  return value !== process.env[key]
    ? describeFn(description, callback)
    : describe.skip(description, callback)
}

/**
 * Conditional describes for SASL OAUTHBEARER.
 * OAUTHBEARER must be enabled as a special case as current Kafka impl
 * doesn't allow it to be enabled aside of other SASL mechanisms.
 */
const describeIfOauthbearerEnabled = describeIfEnv('OAUTHBEARER_ENABLED', '1')
const describeIfOauthbearerDisabled = describeIfNotEnv('OAUTHBEARER_ENABLED', '1')

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
  saslOAuthBearerConnectionOpts,
  saslEntries,
  createConnectionPool,
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
  testIfKafkaAtMost_0_10,
  testIfKafkaAtLeast_0_11,
  testIfKafkaAtLeast_1_1_0,
  flakyTest,
  describeIfOauthbearerEnabled,
  describeIfOauthbearerDisabled,
  addPartitions,
  unsupportedVersionResponse,
  generateMessages,
  unsupportedVersionResponseWithTimeout,
}
