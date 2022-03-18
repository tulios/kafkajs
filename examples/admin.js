const fs = require('fs')
const ip = require('ip')
const tls = require('tls')
const net = require('net')
const http = require('http')

const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || ip.address()
const proxy = {
  host: process.env.HTTP_PROXY_HOST,
  port: parseInt(process.env.HTTP_PROXY_PORT, 10),
}

const socketFactory = ({ host, port, ssl, onConnect }) => {
  const socket = net.connect({ ...proxy }, () => {
    console.log('Connected')
    // Connected to proxy
    const clientRequest = http.request({
      ...proxy,
      createConnection: () => socket,
      method: 'CONNECT',
      path: `${host}:${port}`,
      headers: {
        host: proxy.host,
      },
    })

    clientRequest.on('connect', (res, socket, head) => {
      console.log('Response', res)
      if (res.statusCode !== 200) {
        socket.emit('error', new Error(`Tunneling connection failed: ${res.statusCode}`))
        return
      }

      if (ssl) {
        const tlsSocket = new tls.TLSSocket(socket)
        tls.connect({ socket: tlsSocket, servername: host, ...ssl }, onConnect)
      } else {
        onConnect()
      }
    })

    clientRequest.on('error', err => {
      socket.emit('error', err)
    })

    clientRequest.end()
  })

  return socket
}

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
  socketFactory,

  connectionTimeout: 5000,
})

const admin = kafka.admin()

const run = async () => {
  await admin.connect()
  const topics = await admin.listTopics()
  admin.logger().info('Got topics', { topics })
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
