const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('../index')
const sleep = require('../src/utils/sleep')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'example-consumer',
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

const topic = 'topic-test'
const groupId = 'test-group'

const topics = [topic, `${topic}-5-min`, `${topic}-10-min`, `${topic}-dlq`]
const delays = [60 * 1000, 2 * 60 * 1000]
// const delays = [5 * 1000, 10 * 1000]

const heartbeatInterval = 10000
const consumers = topics
  .slice(0, -1)
  .map(topic => kafka.consumer({ groupId: `${groupId}-${topic}`, heartbeatInterval }))
const producer = kafka.producer()
const admin = kafka.admin()

const createTopics = async () => {
  await admin.connect()
  await admin.createTopics({
    topics: topics.map(topic => ({
      topic,
      numPartitions: 3,
      replicationFactor: 1,
    })),
  })
  await admin.disconnect()
}

const getRandomNumber = () => Math.round(Math.random() * 1000)
const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
  headers: {
    'correlation-id': `${num}-${Date.now()}`,
  },
})

let msgNumber = 0
let requestNumber = 0
const sendMessage = () => {
  const messages = Array(getRandomNumber())
    .fill()
    .map(_ => createMessage(getRandomNumber()))

  const requestId = requestNumber++
  msgNumber += messages.length
  kafka.logger().info(`Sending ${messages.length} messages #${requestId}...`)
  return producer
    .send({
      topic,
      messages,
    })
    .then(response => {
      kafka.logger().info(`Messages sent #${requestId}`, {
        response,
        msgNumber,
      })
    })
    .catch(e => kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack }))
}

const metrics = topics.reduce(
  (acc, topic) => ({
    ...acc,
    [topic]: {
      received: 0,
      processed: 0,
      failed: 0,
    },
  }),
  {}
)

const run = async () => {
  await createTopics()

  await producer.connect()

  // Seeding the topic
  await sendMessage()

  await Promise.all(
    consumers.map(async (consumer, index) => {
      await consumer.connect()
      await consumer.subscribe({ topic: topics[index], fromBeginning: true })

      await consumer.run({
        partitionsConsumedConcurrently: 3,
        eachMessage: async ({ topic, partition, message, heartbeat }) => {
          try {
            metrics[topic].received++
            const headers = message.headers
            const processAt = headers['x-process-at'] ? parseInt(headers['x-process-at'], 10) : null

            if (processAt) {
              const delay = processAt - Date.now()
              consumer.logger().info('Waiting for message to be processed...', {
                topic,
                partition,
                offset: message.offset,
                processAt,
                delay,
              })
              const interval = setInterval(async () => {
                try {
                  consumer.logger().info('Heartbeating while waiting to process message', {
                    topic,
                    partition,
                    processAt,
                    remaining: processAt - Date.now(),
                  })
                  await heartbeat()
                } catch (error) {
                  consumer.logger().warn('Heartbeat error', { error })
                }
              }, heartbeatInterval)
              await sleep(delay)
              clearInterval(interval)
            }

            const shouldFail = Math.random() > 0.9

            if (shouldFail) {
              throw new Error(`Failing on purpose for topic ${topic}:${partition}`)
            }

            metrics[topic].processed++
            consumer.logger().debug('Successfully processed message', {
              topic,
              partition,
              offset: message.offset,
            })
          } catch (error) {
            consumer
              .logger()
              .warn('Error while processing message', { error: error.message || error })
            metrics[topic].failed++

            const dlqTopic = topics[index + 1]
            if (!dlqTopic) {
              consumer.logger().error('No DLQ topic configured!')
              throw error
            }

            const wrappedMessage = {
              partition: message.partition,
              value: message.value,
              headers: {
                ...message.headers,
                'x-process-at': String(Date.now() + delays[index]),
              },
            }
            await producer.send({
              topic: dlqTopic,
              messages: [wrappedMessage],
            })
          }
        },
      })
    })
  )
}

run().catch(e => kafka.logger().error(`[example/consumer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await Promise.all(consumers.map(consumer => consumer.disconnect()))
      await producer.disconnect()
      kafka.logger().info('Metrics', metrics)
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/consumer] disconnecting')
    await Promise.all(consumers.map(consumer => consumer.disconnect()))
    await producer.disconnect()
    kafka.logger().info('Metrics', metrics)
  })
})
