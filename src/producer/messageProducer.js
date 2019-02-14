const createSendMessages = require('./sendMessages')
const { KafkaJSNonRetriableError } = require('../errors')
const convertibleToBuffer = require('../utils/convertibleToBuffer')

module.exports = ({ logger, cluster, partitioner, eosManager, idempotent, retrier }) => {
  const sendMessages = createSendMessages({
    logger,
    cluster,
    partitioner,
    eosManager,
  })

  /**
   * @typedef {Object} TopicMessages
   * @property {string} topic
   * @property {Array} messages An array of objects with "key" and "value", example:
   *                         [{ key: 'my-key', value: 'my-value'}]
   *
   * @typedef {Object} SendBatchRequest
   * @property {Array<TopicMessages>} topicMessages
   * @property {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   *
   * @property {number} [timeout=30000] The time to await a response in ms
   * @property {Compression.Types} [compression=Compression.Types.None] Compression codec
   *
   * @param {SendBatchRequest}
   * @returns {Promise}
   */
  const sendBatch = async ({ acks = -1, timeout, compression, topicMessages = [] }) => {
    if (topicMessages.length === 0 || topicMessages.some(({ topic }) => !topic)) {
      throw new KafkaJSNonRetriableError(`Invalid topic`)
    }

    if (idempotent && acks !== -1) {
      throw new KafkaJSNonRetriableError(
        `Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees`
      )
    }

    for (let { topic, messages } of topicMessages) {
      if (!messages) {
        throw new KafkaJSNonRetriableError(
          `Invalid messages array [${messages}] for topic "${topic}"`
        )
      }

      const messageWithoutValue = messages.find(message => message.value === undefined)
      if (messageWithoutValue) {
        throw new KafkaJSNonRetriableError(
          `Invalid message without value for topic "${topic}": ${JSON.stringify(
            messageWithoutValue
          )}`
        )
      }

      for (let message of messages) {
        if (message.key && !convertibleToBuffer(message.key)) {
          throw new KafkaJSNonRetriableError(
            `Invalid message for topic "${topic}". "key" needs to be convertible to Buffer: ${JSON.stringify(
              message
            )}`
          )
        }

        if (message.value !== null && !convertibleToBuffer(message.value)) {
          throw new KafkaJSNonRetriableError(
            `Invalid message for topic "${topic}". "value" needs to be convertible to Buffer: ${JSON.stringify(
              message
            )}`
          )
        }
      }
    }

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        return await sendMessages({
          acks,
          timeout,
          compression,
          topicMessages,
        })
      } catch (error) {
        if (!cluster.isConnected()) {
          logger.debug(`Cluster has disconnected, reconnecting: ${error.message}`, {
            retryCount,
            retryTime,
          })
          await cluster.connect()
          await cluster.refreshMetadata()
          throw error
        }

        // This is necessary in case the metadata is stale and the number of partitions
        // for this topic has increased in the meantime
        if (
          error.name === 'KafkaJSConnectionError' ||
          (error.name === 'KafkaJSProtocolError' && error.retriable)
        ) {
          logger.error(`Failed to send messages: ${error.message}`, { retryCount, retryTime })
          await cluster.refreshMetadata()
          throw error
        }

        // Skip retries for errors not related to the Kafka protocol
        logger.error(`${error.message}`, { retryCount, retryTime })
        bail(error)
      }
    })
  }

  /**
   * @param {ProduceRequest} ProduceRequest
   * @returns {Promise}
   *
   * @typedef {Object} ProduceRequest
   * @property {string} topic
   * @property {Array} messages An array of objects with "key" and "value", example:
   *                         [{ key: 'my-key', value: 'my-value'}]
   * @property {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   * @property {number} [timeout=30000] The time to await a response in ms
   * @property {Compression.Types} [compression=Compression.Types.None] Compression codec
   */
  const send = async ({ acks, timeout, compression, topic, messages }) => {
    const topicMessage = { topic, messages }
    return sendBatch({
      acks,
      timeout,
      compression,
      topicMessages: [topicMessage],
    })
  }

  return {
    send,
    sendBatch,
  }
}
