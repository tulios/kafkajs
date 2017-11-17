const fs = require('fs')
const ip = require('ip')
const Kafka = require('../src/index')
const { LEVELS } = require('../src/loggers')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: LEVELS.INFO,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'example-consumer',
  ssl: {
    servername: 'localhost',
    cert: fs.readFileSync('./testHelpers/certs/client_cert.pem', 'utf-8'),
    key: fs.readFileSync('./testHelpers/certs/client_key.pem', 'utf-8'),
    ca: [fs.readFileSync('./testHelpers/certs/ca_cert.pem', 'utf-8')],
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
  await consumer.subscribe({ topic })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `- ${topic}[${partition} | ${message.offset}] / ${message.timestamp} ${message.key}#${message.value}`
      )
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
