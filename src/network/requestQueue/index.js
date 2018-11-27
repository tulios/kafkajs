const SocketRequest = require('./socketRequest')

module.exports = class RequestQueue {
  /**
   * @param {number} maxInFlightRequests
   * @param {number} requestTimeout
   * @param {string} clientId
   * @param {string} broker
   * @param {Logger} logger
   */
  constructor({ maxInFlightRequests, requestTimeout, clientId, broker, logger }) {
    this.maxInFlightRequests = maxInFlightRequests
    this.requestTimeout = requestTimeout
    this.clientId = clientId
    this.broker = broker
    this.logger = logger

    this.inflight = new Map()
    this.pending = []
  }

  /**
   * @typedef {Object} PushedRequest
   * @property {RequestEntry} entry
   * @property {boolean} expectResponse
   * @property {Function} sendRequest
   *
   * @public
   * @param {PushedRequest} pushedRequest
   */
  push(pushedRequest) {
    const { correlationId } = pushedRequest.entry
    const socketRequest = new SocketRequest({
      entry: pushedRequest.entry,
      expectResponse: pushedRequest.expectResponse,
      broker: this.broker,
      requestTimeout: this.requestTimeout,
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
      this.logger.debug(`Request enqueued`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId,
      })

      this.pending.push(socketRequest)
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

    for (let socketRequest of requests) {
      socketRequest.rejected(error)
      this.inflight.delete(socketRequest.correlationId)
    }

    this.pending = []
    this.inflight.clear()
  }
}
