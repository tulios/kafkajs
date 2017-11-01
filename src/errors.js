class KafkaJSError extends Error {
  constructor(e, { retriable = true } = {}) {
    super(e.message || e)
    this.name = this.constructor.name
    this.retriable = retriable
  }
}

class KafkaJSPartialMessageError extends KafkaJSError {
  constructor(e) {
    super(e.message || e, { retriable: false })
  }
}

class KafkaJSProtocolError extends KafkaJSError {
  constructor(e) {
    super(e.message, { retriable: e.retriable })
    this.type = e.type
    this.code = e.code
  }
}

class KafkaJSBrokerNotFound extends KafkaJSError {}

module.exports = {
  KafkaJSError,
  KafkaJSPartialMessageError,
  KafkaJSBrokerNotFound,
  KafkaJSProtocolError,
}
