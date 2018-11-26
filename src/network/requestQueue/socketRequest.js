const { KafkaJSRequestTimeoutError } = require('../../errors')

const PRIVATE = {
  STATE: Symbol('private:SocketRequest:state'),
}

const REQUEST_STATE = {
  PENDING: Symbol('PENDING'),
  SENT: Symbol('SENT'),
  COMPLETED: Symbol('COMPLETED'),
  REJECTED: Symbol('REJECTED'),
}

/**
 * @typedef {Object} SocketRequest
 * @property {number} createdAt
 * @property {number} sentAt
 * @property {number} pendingDuration
 * @property {number} duration
 * @property {number} requestTimeout
 * @property {string} broker
 * @property {RequestEntry} entry
 * @property {boolean} expectResponse
 * @property {Function} send
 *
 * @typedef {Object} RequestEntry
 * @property {string} apiKey
 * @property {string} apiName
 * @property {number} apiVersion
 * @property {number} correlationId
 * @property {Function} resolve
 * @property {Function} reject
 */
module.exports = class SocketRequest {
  /**
   * @param {number} requestTimeout
   * @param {string} broker
   * @param {RequestEntry} entry
   * @param {boolean} expectResponse
   * @param {Function} send
   */
  constructor({ requestTimeout, broker, entry, expectResponse, send }) {
    this.createdAt = Date.now()
    this.requestTimeout = requestTimeout
    this.broker = broker
    this.entry = entry
    this.correlationId = entry.correlationId
    this.expectResponse = expectResponse
    this.sendRequest = send

    this.timeoutHandler = null
    this.sentAt = null
    this.duration = null
    this.pendingDuration = null
    this.timeoutId = null

    this[PRIVATE.STATE] = REQUEST_STATE.PENDING
  }

  onTimeout(handler) {
    this.timeoutHandler = handler
  }

  send() {
    if (this[PRIVATE.STATE] !== REQUEST_STATE.PENDING) {
      return
    }

    this.sendRequest()
    this.sentAt = Date.now()
    this.pendingDuration = this.sentAt - this.createdAt
    this[PRIVATE.STATE] = REQUEST_STATE.SENT

    const timeoutCallback = () => {
      const { apiName, apiKey, apiVersion } = this.entry
      const requestInfo = `${apiName}(key: ${apiKey}, version: ${apiVersion})`

      this.timeoutHandler()
      this.rejected(
        new KafkaJSRequestTimeoutError(`Request ${requestInfo} timed out`, {
          broker: this.broker,
          correlationId: this.correlationId,
          createdAt: this.createdAt,
          sentAt: this.sentAt,
          pendingDuration: this.pendingDuration,
        })
      )
    }

    this.timeoutId = setTimeout(timeoutCallback, this.requestTimeout)
  }

  completed({ size, payload }) {
    if (this[PRIVATE.STATE] !== REQUEST_STATE.SENT) {
      return
    }

    clearTimeout(this.timeoutId)
    const { entry, correlationId } = this

    this[PRIVATE.STATE] = REQUEST_STATE.COMPLETED
    this.duration = Date.now() - this.sentAt
    entry.resolve({ correlationId, entry, size, payload })
  }

  rejected(error) {
    if (this[PRIVATE.STATE] !== REQUEST_STATE.SENT) {
      return
    }

    clearTimeout(this.timeoutId)
    this[PRIVATE.STATE] = REQUEST_STATE.REJECTED
    this.duration = Date.now() - this.sentAt
    this.entry.reject(error)
  }
}
