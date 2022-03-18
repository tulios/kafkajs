const fs = require('fs')
const ip = require('ip')
const tls = require('tls')
const net = require('tls')
const http = require('http')

const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || ip.address()
const proxy = {
  host: process.env.HTTP_PROXY_HOST,
  port: parseInt(process.env.HTTP_PROXY_PORT, 10),
}

const connectProxy = ({ targetHost, targetPort, sourceSocket, onConnect }) => {
  console.log('IN connectProxy!!!')

  const options = {
    method: 'CONNECT',
    path: `${targetHost}:${targetPort}`,
    headers: ['host', `${targetHost}:${targetPort}`],
  }

  const client = http.request({ ...proxy, ...options })

  console.log('Making connect request', {
    ...options,
    ...proxy,
  })

  client.on('connect', (response, targetSocket, clientHead) => {
    console.log('Connected to proxy')
    // if (sourceSocket.readyState !== 'open') {
    //   console.log('Source socket not open')
    //   targetSocket.destroy()
    //   return
    // }

    targetSocket.on('error', error => {
      console.log(`Chain Destination Socket Error: ${error.stack}`)

      sourceSocket.destroy()
    })

    sourceSocket.on('error', error => {
      console.log(`Chain Source Socket Error: ${error.stack}`)

      targetSocket.destroy()
    })

    if (response.statusCode !== 200) {
      console.log(`Failed to authenticate upstream proxy: ${response.statusCode}`)
      sourceSocket.end()
      return
    }

    if (clientHead.length > 0) {
      targetSocket.destroy(new Error(`Unexpected data on CONNECT: ${clientHead.length} bytes`))
      return
    }

    console.log('tunnelConnectResponded', {
      response,
      socket: targetSocket,
      head: clientHead,
    })

    // sourceSocket.write('')

    sourceSocket.pipe(targetSocket)
    targetSocket.pipe(sourceSocket)
    console.log('Calling onConnect')
    onConnect()

    // Once target socket closes forcibly, the source socket gets paused.
    // We need to enable flowing, otherwise the socket would remain open indefinitely.
    // Nothing would consume the data, we just want to close the socket.
    targetSocket.on('close', () => {
      sourceSocket.resume()

      if (sourceSocket.writable) {
        sourceSocket.end()
      }
    })

    // Same here.
    sourceSocket.on('close', () => {
      targetSocket.resume()

      if (targetSocket.writable) {
        targetSocket.end()
      }
    })
  })

  client.on('error', error => {
    console.error(`Failed to connect to upstream proxy: ${error.stack}`)

    if (sourceSocket.readyState === 'open') {
      sourceSocket.end()
    }
  })

  sourceSocket.on('error', () => {
    client.destroy()
  })

  sourceSocket.on('close', () => {
    client.destroy()
  })

  client.end()
}

const socketFactory = ({ host, port, ssl, onConnect }) => {
  console.log('Creating socket', { host, port, ssl })
  const socket = ssl ? new tls.TLSSocket() : new net.Socket()
  // if (ssl) {
  //   const server = tls.createServer()
  // }
  connectProxy({ targetHost: host, targetPort: port, sourceSocket: socket, onConnect })
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
  await admin.listTopics()
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
