const net = require('net')
const tls = require('tls')

const createRequest = require('./protocol/request')
const Encoder = require('./protocol/encoder')
const Decoder = require('./protocol/decoder')
const createRetry = require('./retry')

const KEEP_ALIVE_DELAY = 60000 // in ms

const createSocket = (host, port, ssl, onConnect) => {
  if (ssl) {
    return tls.connect(Object.assign({ host, port }, ssl), onConnect)
  }

  return net.connect({ host, port }, onConnect)
}

/**
 * @param {string} host
 * @param {number} port
 * @param {Object} logger
 * @param {Object} [ssl=null]
 * @param {Object} [sasl=null]
 * @param {object} [retry={}]
 */
module.exports = class Connection {
  constructor({ host, port, logger, ssl = null, sasl = null, retry = {} }) {
    this.host = host
    this.port = port
    this.logger = logger

    this.ssl = ssl
    this.sasl = sasl

    this.retrier = createRetry(Object.assign({}, retry, { logger }))
    this.broker = `${host}:${port}`

    this.buffer = Buffer.alloc(0)
    this.connected = false
    this.correlationId = 0
    this.pendingQueue = {}
    this.authHandlers = null
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

      this.logger.debug(`Connecting`, { broker: this.broker, ssl: !!this.ssl, sasl: !!this.sasl })
      this.socket = createSocket(this.host, this.port, this.ssl, () => {
        this.connected = true
        resolve()
      })

      this.socket.setKeepAlive(true, KEEP_ALIVE_DELAY)
      this.socket.on('data', data => this.processData(data))

      this.socket.on('end', () => {
        this.connected = false
        if (this.authHandlers) {
          this.authHandlers.onError()
        } else {
          this.logger.error('Kafka server has closed connection', { broker: this.broker })
        }
      })

      this.socket.on('error', e => {
        this.logger.error(`Connection error`, { broker: this.broker, error: e.message })
        this.disconnect()
        reject(e)
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

  authenticate({ request, response }) {
    return new Promise((resolve, reject) => {
      this.authHandlers = {
        onSuccess: rawData => {
          this.authHandlers = null
          resolve(response.parse(response.decode(rawData)))
        },
        onError: () => {
          this.authHandlers = null
          reject(new Error('Connection closed by the server'))
        },
      }

      const requestPayload = request.encode()
      this.socket.write(requestPayload.buffer, 'binary')
    })
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
          {
            broker: this.broker,
            correlationId,
            retryCount,
            retryTime,
            size,
            error: e.message,
          }
        )

        if (e.retriable) throw e
        bail(e)
      }
    })
  }

  processData(rawData) {
    if (this.authHandlers) {
      return this.authHandlers.onSuccess(rawData)
    }

    this.buffer = Buffer.concat([this.buffer, rawData])
    if (Buffer.byteLength(this.buffer) <= Decoder.int32Size()) {
      return
    }

    const data = Buffer.from(this.buffer)
    this.buffer = Buffer.alloc(0)

    const decoder = new Decoder(data)
    const size = decoder.readInt32()
    const correlationId = decoder.readInt32()
    const payload = decoder.readAll()

    const entry = this.pendingQueue[correlationId]
    delete this.pendingQueue[correlationId]

    if (!entry) {
      this.logger.debug(`Response without match`, { broker: this.broker, correlationId })
      return
    }

    entry.resolve({
      correlationId,
      size,
      entry,
      payload,
    })
  }
}
