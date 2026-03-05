/**
 * Dead Letter Queue (DLQ) wrapper for KafkaJS consumer message handlers.
 *
 * Wraps an eachMessage handler with retry + DLQ logic: if the handler fails
 * after the configured number of retries, the message is produced to a
 * dead-letter topic instead of crashing the consumer.
 *
 * @param {object} options
 * @param {import('../../types').Producer} options.producer - A connected KafkaJS producer
 * @param {string} options.topic - The DLQ topic name
 * @param {number} [options.maxRetries=3] - Max retries before sending to DLQ
 * @param {(message: import('../../types').EachMessagePayload, error: Error) => Promise<void>} [options.onOriginalMessageFailed] - Optional callback when a message is sent to DLQ
 * @param {(error: Error, message: import('../../types').EachMessagePayload) => import('../../types').Message} [options.createDLQMessage] - Optional custom DLQ message builder
 * @returns {(handler: import('../../types').EachMessageHandler) => import('../../types').EachMessageHandler}
 */
module.exports = ({
  producer,
  topic: dlqTopic,
  maxRetries = 3,
  onOriginalMessageFailed,
  createDLQMessage,
}) => {
  if (!producer) {
    throw new Error('deadLetterQueue requires a producer instance')
  }

  if (!dlqTopic) {
    throw new Error('deadLetterQueue requires a topic for the dead letter queue')
  }

  return handler => {
    return async payload => {
      let lastError

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          await handler(payload)
          return
        } catch (error) {
          lastError = error

          if (attempt < maxRetries) {
            continue
          }
        }
      }

      const { topic, partition, message } = payload

      const dlqMessage = createDLQMessage
        ? createDLQMessage(lastError, payload)
        : {
            key: message.key,
            value: message.value,
            headers: {
              ...message.headers,
              'dlq.original.topic': topic,
              'dlq.original.partition': String(partition),
              'dlq.original.offset': message.offset,
              'dlq.original.timestamp': message.timestamp,
              'dlq.error.message': lastError.message,
              'dlq.error.name': lastError.name,
            },
          }

      await producer.send({
        topic: dlqTopic,
        messages: [dlqMessage],
      })

      if (onOriginalMessageFailed) {
        await onOriginalMessageFailed(payload, lastError)
      }
    }
  }
}
