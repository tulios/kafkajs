const createRetry = require('../retry')
const createSocket = require('./socket')
const createRequest = require('../protocol/request')
const Decoder = require('../protocol/decoder')
const { KafkaJSConnectionError } = require('../errors')

/**
 * @param {string} host
 * @param {number} port
 * @param {Object} logger
 * @param {string} clientId='kafkajs'
 * @param {string} [rack=null]
 * @param {Object} [ssl=null] Options for the TLS Secure Context. It accepts all options,
 *                            usually "cert", "key" and "ca". More information at
 *                            https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options
 * @param {Object} [sasl=null] Attributes used for SASL authentication. Options based on the
 *                             key "mechanism". Connection is not actively using the SASL attributes
 *                             but acting as a data object for this information
 * @param {number} [connectionTimeout=1000] The connection timeout, in milliseconds
 * @param {Object} [retry=null] Configurations for the built-in retry mechanism. More information at the
 *                              retry module inside network
 */
module.exports = class Connection {
  constructor({
    host,
    port,
    logger,
    rack = null,
    ssl = null,
    sasl = null,
    clientId = 'kafkajs',
    connectionTimeout = 1000,
    retry = {},
  }) {
    this.host = host
    this.port = port
    this.rack = rack
    this.clientId = clientId
    this.logger = logger

    this.ssl = ssl
    this.sasl = sasl

    this.retry = retry
    this.retrier = createRetry(Object.assign({}, this.retry))
    this.connectionTimeout = connectionTimeout

    this.buffer = Buffer.alloc(0)
    this.connected = false
    this.correlationId = 0
    this.pendingQueue = {}
    this.authHandlers = null

    const log = level => (message, extra = {}) => {
      const logFn = this.logger[level]
      logFn(message, Object.assign({ broker: `${this.host}:${this.port}`, clientId }, extra))
    }

    this.logDebug = log('debug')
    this.logError = log('error')
  }

  /**
   * @public
   * @returns {Promise}
   */
  connect() {
    return new Promise((resolve, reject) => {
      if (this.connected) {
        return resolve(true)
      }

      let timeoutId

      const onConnect = () => {
        clearTimeout(timeoutId)
        this.connected = true
        resolve(true)
      }

      const onData = data => {
        this.processData(data)
      }

      const onEnd = async () => {
        const wasConnected = this.connected
        await this.disconnect()

        if (this.authHandlers) {
          this.authHandlers.onError()
        } else if (wasConnected) {
          this.logDebug('Kafka server has closed connection')
          this.rejectRequests(new KafkaJSConnectionError('Closed connection'))
        }
      }

      const onError = async e => {
        const error = new KafkaJSConnectionError(`Connection error: ${e.message}`)
        this.logError(error.message)
        await this.disconnect()
        this.rejectRequests(error)

        reject(error)
      }

      const onTimeout = async () => {
        await this.disconnect()
        const error = new KafkaJSConnectionError('Connection timeout')
        this.logError(error.message)
        reject(error)
      }

      this.logDebug(`Connecting`, {
        ssl: !!this.ssl,
        sasl: !!this.sasl,
      })

      try {
        timeoutId = setTimeout(onTimeout, this.connectionTimeout)
        this.socket = createSocket({
          host: this.host,
          port: this.port,
          ssl: this.ssl,
          onConnect,
          onData,
          onEnd,
          onError,
        })
      } catch (e) {
        reject(new KafkaJSConnectionError(`Failed to connect: ${e.message}`))
      }
    })
  }

  /**
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    if (!this.connected) {
      return true
    }

    this.logDebug('disconnecting...')
    this.connected = false
    this.socket.end()
    this.socket.unref()
    this.logDebug('disconnected')
    return true
  }

  /**
   * @public
   * @returns {Promise}
   */
  authenticate({ request, response }) {
    return new Promise(async (resolve, reject) => {
      this.authHandlers = {
        onSuccess: rawData => {
          this.authHandlers = null
          response
            .decode(rawData)
            .then(data => response.parse(data))
            .then(resolve)
        },
        onError: () => {
          this.authHandlers = null
          reject(new KafkaJSConnectionError('Connection closed by the server'))
        },
      }

      const requestPayload = await request.encode()
      this.socket.write(requestPayload.buffer, 'binary')
    })
  }

  /**
   * @public
   * @param {object} request It is defined by the protocol and consists of an object with "apiKey",
   *                         "apiVersion", "apiName" and an "encode" function. The encode function
   *                         must return an instance of Encoder
   * @param {object} response It is defined by the protocol and consists of an object with two functions:
   *                          "decode" and "parse"
   * @returns {Promise<data>} where data is the return of "response#parse"
   */
  send({ request, response }) {
    if (!this.connected) {
      return Promise.reject(new KafkaJSConnectionError('Not connected'))
    }

    const requestInfo = ({ apiName, apiKey, apiVersion }) =>
      `${apiName}(key: ${apiKey}, version: ${apiVersion})`

    const sendRequest = async (retryCount, retryTime) => {
      const { clientId } = this
      const correlationId = this.nextCorrelationId()

      const requestPayload = await createRequest({ request, correlationId, clientId })
      const { apiKey, apiName, apiVersion } = request
      this.logDebug(`Request ${requestInfo(request)}`, {
        correlationId,
        retryCount,
        retryTime,
        size: Buffer.byteLength(requestPayload.buffer),
      })

      return new Promise((resolve, reject) => {
        try {
          this.pendingQueue[correlationId] = { apiKey, apiName, apiVersion, resolve, reject }
          this.socket.write(requestPayload.buffer, 'binary')
        } catch (e) {
          reject(e)
        }
      })
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      const { correlationId, size, entry, payload } = await sendRequest(retryCount, retryTime)
      try {
        const payloadDecoded = await response.decode(payload)
        const data = await response.parse(payloadDecoded)
        const isFetchApi = entry.apiName === 'Fetch'
        this.logDebug(`Response ${requestInfo(entry)}`, {
          correlationId,
          size,
          data: isFetchApi ? '[filtered]' : data,
        })

        return data
      } catch (e) {
        this.logError(`Response ${requestInfo(entry)}`, {
          error: e.message,
          correlationId,
          retryCount,
          retryTime,
          size,
        })

        if (this.connected && e.retriable) throw e
        bail(e)
      }
    })
  }

  /**
   * @private
   */
  nextCorrelationId() {
    if (this.correlationId === Number.MAX_VALUE) {
      this.correlationId = 0
    }

    return this.correlationId++
  }

  /**
   * @private
   */
  processData(rawData) {
    if (this.authHandlers) {
      return this.authHandlers.onSuccess(rawData)
    }

    this.buffer = Buffer.concat([this.buffer, rawData])
    // Not enough bytes to read the payload size, keep buffering
    if (Buffer.byteLength(this.buffer) <= Decoder.int32Size()) {
      return
    }

    const data = Buffer.from(this.buffer)
    const decoder = new Decoder(data)
    const size = decoder.readInt32()

    // The full payload is not yet loaded, keep buffering
    if (Buffer.byteLength(this.buffer) <= size) {
      return
    }

    // The full payload is loaded, erase the buffer
    this.buffer = Buffer.alloc(0)
    const correlationId = decoder.readInt32()
    const payload = decoder.readAll()

    const entry = this.pendingQueue[correlationId]
    delete this.pendingQueue[correlationId]

    if (!entry) {
      this.logDebug(`Response without match`, { correlationId })
      return
    }

    entry.resolve({
      correlationId,
      size,
      entry,
      payload,
    })
  }

  /**
   * @private
   */
  rejectRequests(error) {
    Object.keys(this.pendingQueue).forEach(request => {
      this.pendingQueue[request].reject(error)
    })
  }
}
