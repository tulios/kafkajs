class KafkaJSError extends Error {
  constructor(e, { retriable = true } = {}) {
    super(e.message || e)
    this.name = this.constructor.name
    this.retriable = retriable
  }
}

class KafkaJSNonRetriableError extends KafkaJSError {
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

class KafkaJSPartialMessageError extends KafkaJSNonRetriableError {}
class KafkaJSSASLAuthenticationError extends KafkaJSNonRetriableError {}
class KafkaJSConnectionError extends KafkaJSError {}
class KafkaJSBrokerNotFound extends KafkaJSError {}

module.exports = {
  KafkaJSError,
  KafkaJSPartialMessageError,
  KafkaJSBrokerNotFound,
  KafkaJSProtocolError,
  KafkaJSConnectionError,
  KafkaJSSASLAuthenticationError,
}
