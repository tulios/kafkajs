const fs = require('fs')
const ip = require('ip')
const Kafka = require('./src/index')
const { Types } = require('./src/compression')
const { LEVELS } = require('./src/loggers/console')

const kafka = new Kafka({
  logLevel: LEVELS.DEBUG,
  host: process.env.HOST_IP || ip.address(),
  port: 9094,
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
const producer = kafka.producer()

const getRandomNumber = () => Math.round(Math.random(10) * 1000)
const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
})

const sendMessage = () => {
  producer
    .send({
      topic,
      compression: Types.GZIP,
      messages: [createMessage(getRandomNumber())],
    })
    .then(console.log)
    .catch(console.error)
}

const run = async () => {
  await producer.connect()
  setInterval(sendMessage, 1000)
}

run().catch(console.error)

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
