const net = require('net')
const createRequest = require('./protocol/request')
const Encoder = require('./protocol/encoder')
const Decoder = require('./protocol/decoder')
const createRetry = require('./retry')

const processData = pendingQueue => data => {
  const decoder = new Decoder(data)
  const size = decoder.readInt32()
  const correlationId = decoder.readInt32()
  const payload = decoder.readAll()

  const entry = pendingQueue[correlationId]
  delete pendingQueue[correlationId]

  if (!entry) {
    console.error(`Response without match`, { correlationId })
    return
  }

  entry.resolve({
    correlationId,
    size,
    entry,
    payload,
  })
}

module.exports = class Connection {
  constructor({ host, port, retry = {} }) {
    this.host = host
    this.port = port
    this.retrier = createRetry(retry)

    this.connected = false
    this.correlationId = 0
    this.pendingQueue = {}
  }

  nextCorrelationId() {
    if (this.correlationId === Number.MAX_VALUE) {
      this.correlationId = 0
    }

    return this.correlationId++
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.connect({ host: this.host, port: this.port }, () => {
        this.connected = true
        resolve()
      })

      this.socket.on('data', processData(this.pendingQueue))
      this.socket.on('end', () => {
        console.log('Kafka server has closed connection')
        this.disconnect()
      })

      this.socket.on('error', error => {
        console.error(`Connection error`, error)
        reject(error)
        this.disconnect()
      })
    })
  }

  disconnect() {
    console.log('disconnecting...')
    this.connected && this.socket.end()
    this.connected = false
    console.log('disconnected')
  }

  send({ request, response }) {
    const sendRequest = async () => {
      const correlationId = this.nextCorrelationId()

      console.log(
        `Request ${request.apiName}(key: ${request.apiKey}, version: ${request.apiVersion}) correlationId: ${correlationId}`
      )

      const encoder = new Encoder()
      const requestPayload = createRequest({
        request,
        correlationId,
        clientId: 'abc',
      })

      return new Promise(resolve => {
        this.pendingQueue[correlationId] = {
          apiKey: request.apiKey,
          apiName: request.apiName,
          apiVersion: request.apiVersion,
          resolve,
        }
        this.socket.write(requestPayload.buffer, 'binary')
      })
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      const { correlationId, size, entry, payload } = await sendRequest()
      try {
        const data = response.parse(response.decode(payload))

        console.log(
          `Response ${entry.apiName}(key: ${entry.apiKey}, version: ${entry.apiVersion})`,
          { correlationId, size }
        )

        return data
      } catch (e) {
        console.error(
          `Response ${entry.apiName}(key: ${entry.apiKey}, version: ${entry.apiVersion}) - Error: ${e.message}`,
          { correlationId, retryCount, retryTime, size }
        )

        if (e.retriable) throw e
        bail(e)
      }
    })
  }
}
