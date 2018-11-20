const { KafkaJSRequestTimeoutError } = require('../errors')

module.exports = class InFlightRequest {
  constructor({ requestTimeout, broker, entry, send, onTimeout }) {
    this.createdAt = Date.now()
    this.requestTimeout = requestTimeout
    this.broker = broker
    this.entry = entry
    this.correlationId = entry.correlationId
    this.sendRequest = send
    this.onTimeout = onTimeout

    this.sentAt = null
    this.duration = null
    this.pendingDuration = null
    this.timeoutId = null
  }

  send() {
    this.sendRequest()
    this.sentAt = Date.now()
    this.pendingDuration = this.sentAt - this.createdAt

    const timeoutHandler = () => {
      const { apiName, apiKey, apiVersion } = this.entry
      const requestInfo = `${apiName}(key: ${apiKey}, version: ${apiVersion})`

      this.onTimeout()
      this.rejected(
        new KafkaJSRequestTimeoutError(`Request ${requestInfo} timed out`, {
          broker: this.broker,
          correlationId: this.correlationId,
        })
      )
    }

    this.timeoutId = setTimeout(timeoutHandler, this.requestTimeout)
  }

  completed({ size, payload }) {
    clearTimeout(this.timeoutId)
    const { entry, correlationId } = this

    this.duration = Date.now() - this.sentAt
    entry.resolve({ correlationId, entry, size, payload })
  }

  rejected(error) {
    clearTimeout(this.timeoutId)
    this.entry.reject(error)
  }
}
