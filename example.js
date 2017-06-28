const ip = require('ip')
const Kafka = require('./src/index')
const { LEVELS: { DEBUG } } = require('./src/loggers/console')

const kafka = new Kafka({
  host: process.env.HOST_IP || ip.address(),
  port: 9092,
  logLevel: DEBUG,
})

const producer = kafka.producer()
producer
  .connect()
  .then(async () => {
    const r0 = await producer.send({
      topic: 'test-topic',
      messages: new Array(100).fill().map(() => {
        const num = Math.round(Math.random(10) * 1000)
        return { key: `key-${num}`, value: `some-value-${num}` }
      }),
    })

    console.log(JSON.stringify(r0))
  })
  .catch(e => {
    console.error(e)
    producer.disconnect()
  })

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['exit', 'SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
