class KafkaJSError extends Error {
  constructor(e, { retriable = true } = {}) {
    super(e)
    Error.captureStackTrace(this, this.constructor)
    this.message = e.message || e
    this.name = this.constructor.name
    this.retriable = retriable
  }
}

class KafkaJSNonRetriableError extends KafkaJSError {
  constructor(e) {
    super(e, { retriable: false })
  }
}

class KafkaJSProtocolError extends KafkaJSError {
  constructor(e) {
    super(e, { retriable: e.retriable })
    this.type = e.type
    this.code = e.code
  }
}

class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  constructor(e, { topic, partition }) {
    super(e)
    this.topic = topic
    this.partition = partition
  }
}

class KafkaJSNumberOfRetriesExceeded extends KafkaJSNonRetriableError {
  constructor(e, { retryCount, retryTime }) {
    super(e)
    this.originalError = e
    this.retryCount = retryCount
    this.retryTime = retryTime
  }
}

class KafkaJSConnectionError extends KafkaJSError {
  constructor(e, { broker } = {}) {
    super(e)
    this.broker = broker
  }
}

class KafkaJSTopicMetadataNotLoaded extends KafkaJSError {
  constructor(e, { topic } = {}) {
    super(e)
    this.topic = topic
  }
}

class KafkaJSBrokerNotFound extends KafkaJSError {}
class KafkaJSPartialMessageError extends KafkaJSNonRetriableError {}
class KafkaJSSASLAuthenticationError extends KafkaJSNonRetriableError {}
class KafkaJSGroupCoordinatorNotFound extends KafkaJSNonRetriableError {}
class KafkaJSNotImplemented extends KafkaJSNonRetriableError {}

module.exports = {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSPartialMessageError,
  KafkaJSBrokerNotFound,
  KafkaJSProtocolError,
  KafkaJSConnectionError,
  KafkaJSSASLAuthenticationError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSOffsetOutOfRange,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSNotImplemented,
  KafkaJSTopicMetadataNotLoaded,
}
