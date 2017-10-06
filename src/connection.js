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
    this.logger.debug(`Response without match`, { correlationId })
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
  constructor({ host, port, logger, retry = {} }) {
    this.host = host
    this.port = port
    this.logger = logger
    this.retrier = createRetry(Object.assign({}, retry, { logger }))
    this.broker = `${host}:${port}`

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
      if (this.connected) {
        return resolve()
      }

      this.logger.debug(`Connecting`, { broker: this.broker })
      this.socket = net.connect({ host: this.host, port: this.port }, () => {
        this.connected = true
        resolve()
      })

      this.socket.on('data', processData(this.pendingQueue))
      this.socket.on('end', () => {
        this.logger.error('Kafka server has closed connection', { broker: this.broker })
        this.connected = false
      })

      this.socket.on('error', e => {
        this.logger.error(`Connection error`, { broker: this.broker, error: e.message })
        reject(e)
        this.disconnect()
      })
    })
  }

  disconnect() {
    if (!this.connected) {
      return
    }

    this.logger.debug('disconnecting...', { broker: this.broker })
    this.socket.end()
    this.connected = false
    this.logger.debug('disconnected', { broker: this.broker })
  }

  send({ request, response }) {
    const sendRequest = async (retryCount, retryTime) => {
      const correlationId = this.nextCorrelationId()

      this.logger.debug(
        `Request ${request.apiName}(key: ${request.apiKey}, version: ${request.apiVersion})`,
        { broker: this.broker, correlationId, retryCount, retryTime }
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
      const { correlationId, size, entry, payload } = await sendRequest(retryCount, retryTime)
      try {
        const data = response.parse(response.decode(payload))

        this.logger.debug(
          `Response ${entry.apiName}(key: ${entry.apiKey}, version: ${entry.apiVersion})`,
          { broker: this.broker, correlationId, size, data }
        )

        return data
      } catch (e) {
        this.logger.error(
          `Response ${entry.apiName}(key: ${entry.apiKey}, version: ${entry.apiVersion}) error`,
          Object.assign(
            {
              broker: this.broker,
              correlationId,
              retryCount,
              retryTime,
              size,
              error: e.message,
            },
            e
          )
        )

        if (e.retriable) throw e
        bail(e)
      }
    })
  }
}
