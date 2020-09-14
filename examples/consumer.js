const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('../index')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
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

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
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
      })
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
