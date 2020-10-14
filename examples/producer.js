const fs = require('fs')
const ip = require('ip')
const toxiproxyClient = require('toxiproxy-node-client')

const { Kafka, CompressionTypes, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:39094`],
  clientId: 'example-producer',
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

const toxiproxy = new toxiproxyClient.Toxiproxy(`http://${host}:8474`)

const topic = 'topic-test'
const producer = kafka.producer()

const getRandomNumber = () => Math.round(Math.random(10) * 1000)
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
      compression: CompressionTypes.GZIP,
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

let intervalId
const run = async () => {
  await toxiproxy.reset()

  let proxy

  try {
    proxy = await toxiproxy.createProxy({
      listen: `0.0.0.0:39094`,
      name: 'kafka-proxy',
      upstream: `kafka:29094`,
    })
  } catch (e) {
    if (e.message === 'Proxy kafka-proxy already exists') {
      proxy = await toxiproxy.get('kafka-proxy')
    } else {
      throw e
    }
  }

  producer.logger().info('Initialized proxy', {
    proxy,
  })

  await producer.connect()

  await sendMessage()

  producer.logger().info('Creating network timeout', {
    toxic: (
      await proxy.addToxic(
        new toxiproxyClient.Toxic(proxy, {
          type: 'timeout',
          attributes: {
            timeout: 0,
          },
        })
      )
    ).toJson(),
  })

  await sendMessage()

  quit('SIGTERM')
}

run().catch(e => kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => quit(type, e))
})

signalTraps.map(type => {
  process.once(type, async () => quit(type))
})

const quit = async (type, e) => {
  try {
    kafka.logger().info(`process.on ${type}`)
    if (e) {
      kafka.logger().error(e.message, { stack: e.stack })
    }

    await producer.disconnect()
    process.exit(0)
  } catch (_) {
    process.exit(1)
  }
}
