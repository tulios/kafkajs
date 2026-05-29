const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('../index')
const PartitionAssigners = require('../src/consumer/assigners')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const { cooperativeSticky } = PartitionAssigners

/**
 * Cooperative-sticky incremental rebalancing demo.
 *
 * Produces to a 6-partition topic, starts one consumer, then scales the group
 * up to three consumers. Each consumer logs the `revoked` / `added` partitions
 * carried on every GROUP_JOIN event so you can SEE that scaling up only moves
 * the partitions that must move — not the whole assignment, as eager
 * rebalancing would.
 *
 * Run against the local docker-compose broker:
 *
 *   ./scripts/dockerComposeUp.sh
 *   node examples/cooperativeStickyDemo.js
 */
const host = process.env.HOST_IP || ip.address()

const topic = 'cooperative-sticky-demo'
const groupId = 'cooperative-sticky-demo-group'
const NUM_PARTITIONS = 6

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'cooperative-sticky-demo',
  ssl: {
    servername: 'localhost',
    rejectUnauthorized: false,
    ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'testtest',
  },
})

const logger = kafka.logger()
const consumers = []

const formatPartitions = list =>
  list && list.length ? list.map(({ partition }) => partition).join(',') : '∅'

const createDemoConsumer = label => {
  const consumer = kafka.consumer({
    groupId,
    partitionAssigners: [cooperativeSticky],
    sessionTimeout: 10000,
    rebalanceTimeout: 10000,
    heartbeatInterval: 1000,
    maxWaitTimeInMs: 100,
  })

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    const owned = payload.memberAssignment[topic] || []
    logger.info(`[${label}] rebalanced`, {
      protocol: payload.groupProtocol,
      owns: owned.length ? owned.join(',') : '∅',
      added: formatPartitions(payload.added),
      revoked: formatPartitions(payload.revoked),
    })
  })

  consumers.push({ label, consumer })
  return consumer
}

const startConsumer = async label => {
  const consumer = createDemoConsumer(label)
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async () => {} })
  return consumer
}

const produceMessages = async () => {
  const producer = kafka.producer()
  await producer.connect()
  const messages = Array.from({ length: NUM_PARTITIONS * 10 }, (_, i) => ({
    key: `key-${i}`,
    value: `value-${i}`,
    partition: i % NUM_PARTITIONS,
  }))
  await producer.send({ topic, messages })
  await producer.disconnect()
  logger.info(`Produced ${messages.length} messages across ${NUM_PARTITIONS} partitions`)
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const run = async () => {
  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({
    topics: [{ topic, numPartitions: NUM_PARTITIONS }],
    waitForLeaders: true,
  })
  await admin.disconnect()

  await produceMessages()

  logger.info('--- Phase 1: a single consumer owns every partition ---')
  await startConsumer('consumer-1')
  await sleep(5000)

  logger.info('--- Phase 2: consumer-2 joins; only the partitions that move are revoked ---')
  await startConsumer('consumer-2')
  await sleep(6000)

  logger.info('--- Phase 3: consumer-3 joins; again only a minimal slice moves ---')
  await startConsumer('consumer-3')
  await sleep(6000)

  logger.info('--- Phase 4: consumer-3 leaves; its partitions are redistributed ---')
  const third = consumers.pop()
  await third.consumer.disconnect()
  await sleep(6000)

  logger.info('Demo complete, shutting down')
  await Promise.all(consumers.map(({ consumer }) => consumer.disconnect().catch(() => {})))
  process.exit(0)
}

run().catch(e => {
  logger.error(`[cooperative-sticky-demo] ${e.message}`, { stack: e.stack })
  process.exit(1)
})

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      logger.info(`process.on ${type}`)
      logger.error(e.message, { stack: e.stack })
      await Promise.all(consumers.map(({ consumer }) => consumer.disconnect().catch(() => {})))
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    logger.info('[cooperative-sticky-demo] disconnecting')
    await Promise.all(consumers.map(({ consumer }) => consumer.disconnect().catch(() => {})))
  })
})
