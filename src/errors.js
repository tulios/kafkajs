class KafkaJSError extends Error {
  constructor(e, { retriable = true } = {}) {
    super(e)
    Error.captureStackTrace(this, this.constructor)
    this.message = e.message || e
    this.name = this.constructor.name
    this.retriable = retriable
    this.helpUrl = e.helpUrl
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
    this.stack = `${this.name}\n  Caused by: ${e.stack}`
    this.originalError = e
    this.retryCount = retryCount
    this.retryTime = retryTime
  }
}

class KafkaJSConnectionError extends KafkaJSError {
  constructor(e, { broker, code } = {}) {
    super(e)
    this.broker = broker
    this.code = code
  }
}

class KafkaJSRequestTimeoutError extends KafkaJSError {
  constructor(e, { broker, correlationId, createdAt, sentAt, pendingDuration } = {}) {
    super(e)
    this.broker = broker
    this.correlationId = correlationId
    this.createdAt = createdAt
    this.sentAt = sentAt
    this.pendingDuration = pendingDuration
  }
}

class KafkaJSMetadataNotLoaded extends KafkaJSError {}
class KafkaJSTopicMetadataNotLoaded extends KafkaJSMetadataNotLoaded {
  constructor(e, { topic } = {}) {
    super(e)
    this.topic = topic
  }
}
class KafkaJSStaleTopicMetadataAssignment extends KafkaJSError {
  constructor(e, { topic, unknownPartitions } = {}) {
    super(e)
    this.topic = topic
    this.unknownPartitions = unknownPartitions
  }
}

class KafkaJSServerDoesNotSupportApiKey extends KafkaJSNonRetriableError {
  constructor(e, { apiKey, apiName } = {}) {
    super(e)
    this.apiKey = apiKey
    this.apiName = apiName
  }
}

class KafkaJSBrokerNotFound extends KafkaJSError {}
class KafkaJSPartialMessageError extends KafkaJSNonRetriableError {}
class KafkaJSSASLAuthenticationError extends KafkaJSNonRetriableError {}
class KafkaJSGroupCoordinatorNotFound extends KafkaJSNonRetriableError {}
class KafkaJSNotImplemented extends KafkaJSNonRetriableError {}
class KafkaJSTimeout extends KafkaJSNonRetriableError {}
class KafkaJSLockTimeout extends KafkaJSTimeout {}
class KafkaJSUnsupportedMagicByteInMessageSet extends KafkaJSNonRetriableError {}

module.exports = {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSPartialMessageError,
  KafkaJSBrokerNotFound,
  KafkaJSProtocolError,
  KafkaJSConnectionError,
  KafkaJSRequestTimeoutError,
  KafkaJSSASLAuthenticationError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSOffsetOutOfRange,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSNotImplemented,
  KafkaJSMetadataNotLoaded,
  KafkaJSTopicMetadataNotLoaded,
  KafkaJSStaleTopicMetadataAssignment,
  KafkaJSTimeout,
  KafkaJSLockTimeout,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSUnsupportedMagicByteInMessageSet,
}
