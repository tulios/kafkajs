class KafkaJSError extends Error {
  constructor(e, { retriable = true } = {}) {
    super(e.message || e)
    this.name = 'KafkaJSError'
    this.retriable = retriable
  }
}

module.exports = {
  KafkaJSError,
}
