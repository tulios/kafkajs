const fs = require('fs')
const ip = require('ip')
const Kafka = require('./src/index')
const { Types } = require('./src/protocol/message/compression')
const { LEVELS: { DEBUG } } = require('./src/loggers/console')

const kafka = new Kafka({
  logLevel: DEBUG,
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

const producer = kafka.producer()
producer
  .connect()
  .then(async () => {
    const r0 = await producer.send({
      topic: 'test-topic',
      compression: Types.GZIP,
      messages: new Array(100).fill().map(() => {
        const num = Math.round(Math.random(10) * 1000)
        return { key: `key-${num}`, value: `some-value-${num}` }
      }),
    })

    console.log(JSON.stringify(r0))
  })
  .catch(async e => {
    console.error(e)
    await producer.disconnect()
  })

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
