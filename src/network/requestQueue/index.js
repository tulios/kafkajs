const SocketRequest = require('./socketRequest')

module.exports = class RequestQueue {
  /**
   * @param {number} maxInFlightRequests
   * @param {number} requestTimeout
   * @param {string} clientId
   * @param {string} broker
   * @param {Object} logger
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

  createRequest({ entry, expectResponse, send: sendRequest }) {
    const request = new SocketRequest({
      entry,
      expectResponse,
      broker: this.broker,
      requestTimeout: this.requestTimeout,
      send: () => {
        this.inflight.set(entry.correlationId, request)
        sendRequest()
      },
    })

    return request
  }

  push(request) {
    const { correlationId } = request

    request.onTimeout(() => {
      this.inflight.delete(correlationId)
      const request = this.pending.pop()
      request && request.send()
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

      this.pending.push(request)
      return
    }

    request.send()

    if (!request.expectResponse) {
      this.logger.debug(`Request does not expect a response, resolving immediately`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId: request.correlationId,
      })

      this.inflight.delete(correlationId)
      request.completed({ size: 0, payload: null })
    }
  }

  onResponse({ correlationId, payload, size }) {
    const request = this.inflight.get(correlationId)

    if (this.pending.length > 0) {
      const pendingRequest = this.pending.pop()
      pendingRequest.send()

      this.logger.debug(`Consumed pending request`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId: pendingRequest.correlationId,
        pendingDuration: request.pendingDuration,
        currentPendingQueueSize: this.pending.length,
      })
    }

    this.inflight.delete(correlationId)

    if (!request) {
      this.logger.warn(`Response without match`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId,
      })

      return
    }

    request.completed({ size, payload })
  }

  rejectAll(error) {
    const requests = [...this.inflight.values(), ...this.pending]

    for (let request of requests) {
      request.rejected(error)
      this.inflight.delete(request.correlationId)
    }

    this.pending = []
  }
}
