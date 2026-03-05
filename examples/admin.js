const fs = require('fs')
const os = require('os')

const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const getLocalIP = () => {
  const interfaces = os.networkInterfaces()
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) return iface.address
    }
  }
  return '127.0.0.1'
}

const host = process.env.HOST_IP || getLocalIP()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'test-admin-id',
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

const topic = 'topic-test1'

const admin = kafka.admin()

const run = async () => {
  await admin.connect()
  await admin.createTopics({
    topics: [{ topic }],
    waitForLeaders: true,
  })
  await admin.createPartitions({
    topicPartitions: [{ topic: topic, count: 3 }],
  })
}

run().catch(e => kafka.logger().error(`[example/admin] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await admin.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/admin] disconnecting')
    await admin.disconnect()
  })
})
