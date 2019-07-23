const createRetry = require('../retry')
const createSocket = require('./socket')
const createRequest = require('../protocol/request')
const Decoder = require('../protocol/decoder')
const { KafkaJSConnectionError } = require('../errors')
const { INT_32_MAX_VALUE } = require('../constants')
const getEnv = require('../env')
const RequestQueue = require('./requestQueue')

const requestInfo = ({ apiName, apiKey, apiVersion }) =>
  `${apiName}(key: ${apiKey}, version: ${apiVersion})`

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
 * @param {number} [requestTimeout=30000] The maximum amount of time the client will wait for the response of a request,
 *                                        in milliseconds
 * @param {Object} [retry=null] Configurations for the built-in retry mechanism. More information at the
 *                              retry module inside network
 * @param {number} [maxInFlightRequests=null] The maximum number of unacknowledged requests on a connection before
 *                                            enqueuing
 * @param {InstrumentationEventEmitter} [instrumentationEmitter=null]
 */
module.exports = class Connection {
  constructor({
    host,
    port,
    logger,
    socketFactory,
    rack = null,
    ssl = null,
    sasl = null,
    clientId = 'kafkajs',
    connectionTimeout = 1000,
    requestTimeout = 30000,
    enforceRequestTimeout = false,
    maxInFlightRequests = null,
    instrumentationEmitter = null,
    retry = {},
  }) {
    this.host = host
    this.port = port
    this.rack = rack
    this.clientId = clientId
    this.broker = `${this.host}:${this.port}`
    this.logger = logger.namespace('Connection')

    this.socketFactory = socketFactory
    this.ssl = ssl
    this.sasl = sasl

    this.retry = retry
    this.retrier = createRetry({ ...this.retry })
    this.requestTimeout = requestTimeout
    this.connectionTimeout = connectionTimeout

    this.buffer = Buffer.alloc(0)
    this.connected = false
    this.correlationId = 0
    this.requestQueue = new RequestQueue({
      instrumentationEmitter,
      maxInFlightRequests,
      requestTimeout,
      enforceRequestTimeout,
      clientId,
      broker: this.broker,
      logger: logger.namespace('RequestQueue'),
    })

    this.authHandlers = null
    this.authExpectResponse = false

    const log = level => (message, extra = {}) => {
      const logFn = this.logger[level]
      logFn(message, { broker: this.broker, clientId, ...extra })
    }

    this.logDebug = log('debug')
    this.logError = log('error')
    this.shouldLogBuffers = getEnv().KAFKAJS_DEBUG_PROTOCOL_BUFFERS === '1'
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
        clearTimeout(timeoutId)

        const wasConnected = this.connected
        await this.disconnect()

        if (this.authHandlers) {
          this.authHandlers.onError()
        } else if (wasConnected) {
          this.logDebug('Kafka server has closed connection')
          this.rejectRequests(
            new KafkaJSConnectionError('Closed connection', {
              broker: `${this.host}:${this.port}`,
            })
          )
        }
      }

      const onError = async e => {
        clearTimeout(timeoutId)

        const error = new KafkaJSConnectionError(`Connection error: ${e.message}`, {
          broker: `${this.host}:${this.port}`,
          code: e.code,
        })

        this.logError(error.message, { stack: e.stack })
        await this.disconnect()
        this.rejectRequests(error)

        reject(error)
      }

      const onTimeout = async () => {
        const error = new KafkaJSConnectionError('Connection timeout', {
          broker: `${this.host}:${this.port}`,
        })

        this.logError(error.message)
        await this.disconnect()
        this.rejectRequests(error)
        reject(error)
      }

      this.logDebug(`Connecting`, {
        ssl: !!this.ssl,
        sasl: !!this.sasl,
      })

      try {
        timeoutId = setTimeout(onTimeout, this.connectionTimeout)
        this.socket = createSocket({
          socketFactory: this.socketFactory,
          host: this.host,
          port: this.port,
          ssl: this.ssl,
          onConnect,
          onData,
          onEnd,
          onError,
          onTimeout,
        })
      } catch (e) {
        reject(
          new KafkaJSConnectionError(`Failed to connect: ${e.message}`, {
            broker: `${this.host}:${this.port}`,
          })
        )
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
  authenticate({ authExpectResponse = false, request, response }) {
    this.authExpectResponse = authExpectResponse

    /**
     * TODO: rewrite removing the async promise executor
     */

    /* eslint-disable no-async-promise-executor */
    return new Promise(async (resolve, reject) => {
      this.authHandlers = {
        onSuccess: rawData => {
          this.authHandlers = null
          this.authExpectResponse = false

          response
            .decode(rawData)
            .then(data => response.parse(data))
            .then(resolve)
            .catch(reject)
        },
        onError: () => {
          this.authHandlers = null
          this.authExpectResponse = false

          reject(
            new KafkaJSConnectionError('Connection closed by the server', {
              broker: `${this.host}:${this.port}`,
            })
          )
        },
      }

      try {
        const requestPayload = await request.encode()

        this.failIfNotConnected()
        this.socket.write(requestPayload.buffer, 'binary')
      } catch (e) {
        reject(e)
      }
    })
  }

  /**
   * @public
   * @param {object} request It is defined by the protocol and consists of an object with "apiKey",
   *                         "apiVersion", "apiName" and an "encode" function. The encode function
   *                         must return an instance of Encoder
   *
   * @param {object} response It is defined by the protocol and consists of an object with two functions:
   *                          "decode" and "parse"
   *
   * @param {number} [requestTimeout=null] Override for the default requestTimeout
   * @returns {Promise<data>} where data is the return of "response#parse"
   */
  async send({ request, response, requestTimeout = null }) {
    this.failIfNotConnected()

    const expectResponse = !request.expectResponse || request.expectResponse()
    const sendRequest = async () => {
      const { clientId } = this
      const correlationId = this.nextCorrelationId()

      const requestPayload = await createRequest({ request, correlationId, clientId })
      const { apiKey, apiName, apiVersion } = request
      this.logDebug(`Request ${requestInfo(request)}`, {
        correlationId,
        expectResponse,
        size: Buffer.byteLength(requestPayload.buffer),
      })

      return new Promise((resolve, reject) => {
        try {
          this.failIfNotConnected()
          const entry = { apiKey, apiName, apiVersion, correlationId, resolve, reject }

          this.requestQueue.push({
            entry,
            expectResponse,
            requestTimeout,
            sendRequest: () => {
              this.socket.write(requestPayload.buffer, 'binary')
            },
          })
        } catch (e) {
          reject(e)
        }
      })
    }

    const { correlationId, size, entry, payload } = await sendRequest()

    if (!expectResponse) {
      return
    }

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
      if (entry.apiName !== 'ApiVersions') {
        this.logError(`Response ${requestInfo(entry)}`, {
          error: e.message,
          correlationId,
          size,
        })
      }

      const isBuffer = Buffer.isBuffer(payload)
      this.logDebug(`Response ${requestInfo(entry)}`, {
        error: e.message,
        correlationId,
        payload:
          isBuffer && !this.shouldLogBuffers ? { type: 'Buffer', data: '[filtered]' } : payload,
      })

      throw e
    }
  }

  /**
   * @private
   */
  failIfNotConnected() {
    if (!this.connected) {
      throw new KafkaJSConnectionError('Not connected', {
        broker: `${this.host}:${this.port}`,
      })
    }
  }

  /**
   * @private
   */
  nextCorrelationId() {
    if (this.correlationId >= INT_32_MAX_VALUE) {
      this.correlationId = 0
    }

    return this.correlationId++
  }

  /**
   * @private
   */
  processData(rawData) {
    if (this.authHandlers && !this.authExpectResponse) {
      return this.authHandlers.onSuccess(rawData)
    }

    this.buffer = Buffer.concat([this.buffer, rawData])

    // Process data if there are enough bytes to read the expected response size,
    // otherwise keep buffering
    while (Buffer.byteLength(this.buffer) > Decoder.int32Size()) {
      const data = Buffer.from(this.buffer)
      const decoder = new Decoder(data)
      const expectedResponseSize = decoder.readInt32()

      if (!decoder.canReadBytes(expectedResponseSize)) {
        return
      }

      const response = new Decoder(decoder.readBytes(expectedResponseSize))
      // Reset the buffer as the rest of the bytes
      this.buffer = decoder.readAll()

      if (this.authHandlers) {
        const rawResponseSize = Decoder.int32Size() + expectedResponseSize
        const rawResponseBuffer = data.slice(0, rawResponseSize)
        return this.authHandlers.onSuccess(rawResponseBuffer)
      }

      const correlationId = response.readInt32()
      const payload = response.readAll()

      this.requestQueue.fulfillRequest({
        size: expectedResponseSize,
        correlationId,
        payload,
      })
    }
  }

  /**
   * @private
   */
  rejectRequests(error) {
    this.requestQueue.rejectAll(error)
  }
}
