const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('../index')
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
const consumer = kafka.consumer({ groupId: 'test-group' })

let msgNumber = 0
const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      msgNumber++
      kafka.logger().info('Message processed', {
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
        headers: Object.keys(message.headers).reduce(
          (headers, key) => ({
            ...headers,
            [key]: message.headers[key].toString(),
          }),
          {}
        ),
        key: message.key.toString(),
        value: message.value.toString(),
        msgNumber,
      })
    },
  })
}

run().catch(e => kafka.logger().error(`[example/consumer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await consumer.disconnect()
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
    await consumer.disconnect()
  })
})
