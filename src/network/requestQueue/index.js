const SocketRequest = require('./socketRequest')
const events = require('../instrumentationEvents')

const PRIVATE = {
  EMIT_QUEUE_SIZE_EVENT: Symbol('private:RequestQueue:emitQueueSizeEvent'),
}

module.exports = class RequestQueue {
  /**
   * @param {number} maxInFlightRequests
   * @param {number} requestTimeout
   * @param {string} clientId
   * @param {string} broker
   * @param {Logger} logger
   * @param {InstrumentationEventEmitter} [instrumentationEmitter=null]
   */
  constructor({
    instrumentationEmitter = null,
    maxInFlightRequests,
    requestTimeout,
    enforceRequestTimeout,
    clientId,
    broker,
    logger,
  }) {
    this.instrumentationEmitter = instrumentationEmitter
    this.maxInFlightRequests = maxInFlightRequests
    this.requestTimeout = requestTimeout
    this.enforceRequestTimeout = enforceRequestTimeout
    this.clientId = clientId
    this.broker = broker
    this.logger = logger

    this.inflight = new Map()
    this.pending = []

    this[PRIVATE.EMIT_QUEUE_SIZE_EVENT] = () => {
      instrumentationEmitter &&
        instrumentationEmitter.emit(events.NETWORK_REQUEST_QUEUE_SIZE, {
          broker: this.broker,
          clientId: this.clientId,
          queueSize: this.pending.length,
        })
    }
  }

  /**
   * @typedef {Object} PushedRequest
   * @property {RequestEntry} entry
   * @property {boolean} expectResponse
   * @property {Function} sendRequest
   * @property {number} [requestTimeout]
   *
   * @public
   * @param {PushedRequest} pushedRequest
   */
  push(pushedRequest) {
    const { correlationId } = pushedRequest.entry
    const defaultRequestTimeout = this.requestTimeout
    const customRequestTimeout = pushedRequest.requestTimeout

    // Some protocol requests have custom request timeouts (e.g JoinGroup, Fetch, etc). The custom
    // timeouts are influenced by user configurations, which can be lower than the default requestTimeout
    const requestTimeout = Math.max(defaultRequestTimeout, customRequestTimeout || 0)

    const socketRequest = new SocketRequest({
      entry: pushedRequest.entry,
      expectResponse: pushedRequest.expectResponse,
      broker: this.broker,
      clientId: this.clientId,
      instrumentationEmitter: this.instrumentationEmitter,
      enforceRequestTimeout: this.enforceRequestTimeout,
      requestTimeout,
      send: () => {
        this.inflight.set(correlationId, socketRequest)
        pushedRequest.sendRequest()
      },
      timeout: () => {
        this.inflight.delete(correlationId)
        const pendingRequest = this.pending.pop()
        pendingRequest && pendingRequest.send()
      },
    })

    // TODO: Remove the "null" check once this is validated in production and
    // can receive a default value
    const shouldEnqueue =
      this.maxInFlightRequests != null && this.inflight.size >= this.maxInFlightRequests

    if (shouldEnqueue) {
      this.pending.push(socketRequest)

      this.logger.debug(`Request enqueued`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId,
      })

      this[PRIVATE.EMIT_QUEUE_SIZE_EVENT]()
      return
    }

    socketRequest.send()

    if (!socketRequest.expectResponse) {
      this.logger.debug(`Request does not expect a response, resolving immediately`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId,
      })

      this.inflight.delete(correlationId)
      socketRequest.completed({ size: 0, payload: null })
    }
  }

  /**
   * @public
   * @param {number} correlationId
   * @param {Buffer} payload
   * @param {number} size
   */
  fulfillRequest({ correlationId, payload, size }) {
    const socketRequest = this.inflight.get(correlationId)

    if (this.pending.length > 0) {
      const pendingRequest = this.pending.pop()
      pendingRequest.send()

      this.logger.debug(`Consumed pending request`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId: pendingRequest.correlationId,
        pendingDuration: pendingRequest.pendingDuration,
        currentPendingQueueSize: this.pending.length,
      })

      this[PRIVATE.EMIT_QUEUE_SIZE_EVENT]()
    }

    this.inflight.delete(correlationId)

    if (!socketRequest) {
      this.logger.warn(`Response without match`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId,
      })

      return
    }

    socketRequest.completed({ size, payload })
  }

  /**
   * @public
   * @param {Error} error
   */
  rejectAll(error) {
    const requests = [...this.inflight.values(), ...this.pending]

    for (const socketRequest of requests) {
      socketRequest.rejected(error)
      this.inflight.delete(socketRequest.correlationId)
    }

    this.pending = []
    this.inflight.clear()
    this[PRIVATE.EMIT_QUEUE_SIZE_EVENT]()
  }
}
