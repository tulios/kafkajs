const createSendMessages = require('./sendMessages')
const { KafkaJSError, KafkaJSNonRetriableError } = require('../errors')
const { CONNECTION_STATUS } = require('../network/connectionStatus')

module.exports = ({
  logger,
  cluster,
  partitioner,
  eosManager,
  idempotent,
  retrier,
  getConnectionStatus,
}) => {
  const sendMessages = createSendMessages({
    logger,
    cluster,
    partitioner,
    eosManager,
  })

  const validateConnectionStatus = () => {
    const connectionStatus = getConnectionStatus()

    switch (connectionStatus) {
      case CONNECTION_STATUS.DISCONNECTING:
        throw new KafkaJSNonRetriableError(
          `The producer is disconnecting; therefore, it can't safely accept messages anymore`
        )
      case CONNECTION_STATUS.DISCONNECTED:
        throw new KafkaJSError('The producer is disconnected')
    }
  }

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
    if (topicMessages.some(({ topic }) => !topic)) {
      throw new KafkaJSNonRetriableError(`Invalid topic`)
    }

    if (idempotent && acks !== -1) {
      throw new KafkaJSNonRetriableError(
        `Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees`
      )
    }

    for (const { topic, messages } of topicMessages) {
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
    }

    validateConnectionStatus()
    const mergedTopicMessages = topicMessages.reduce((merged, { topic, messages }) => {
      const index = merged.findIndex(({ topic: mergedTopic }) => topic === mergedTopic)

      if (index === -1) {
        merged.push({ topic, messages })
      } else {
        merged[index].messages = [...merged[index].messages, ...messages]
      }

      return merged
    }, [])

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        return await sendMessages({
          acks,
          timeout,
          compression,
          topicMessages: mergedTopicMessages,
        })
      } catch (error) {
        if (error.name === 'KafkaJSConnectionClosedError') {
          cluster.removeBroker({ host: error.host, port: error.port })
        }

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
          error.name === 'KafkaJSConnectionClosedError' ||
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
