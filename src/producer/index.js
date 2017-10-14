const { KafkaProtocolError } = require('../protocol/error')
const createRetry = require('../retry')
const createDefaultPartitioner = require('./partitioners/default')
const createSendMessages = require('./sendMessages')

module.exports = ({
  cluster,
  createPartitioner = createDefaultPartitioner,
  retry = { retries: 2 },
}) => {
  const partitioner = createPartitioner()
  const retrier = createRetry(Object.assign({}, cluster.retry, retry))
  const sendMessages = createSendMessages({ cluster, partitioner })
  const { logger } = cluster

  return {
    /**
     * @returns {Promise}
     */
    connect: async () => await cluster.connect(),

    /**
     * @return {Promise}
     */
    disconnect: async () => await cluster.disconnect(),

    /**
     * @param {string} topic
     * @param {Array} messages An array of objects with "key" and "value", example:
     *                         [{ key: 'my-key', value: 'my-value'}]
     * @param {number} [acks=-1] Control the number of required acks.
     *                           -1 = all replicas must acknowledge
     *                            0 = no acknowledgments
     *                            1 = only waits for the leader to acknowledge
     * @param {number} [timeout=30000] The time to await a response in ms
     * @param {Compression.Types} [compression=Compression.Types.None] Compression codec
     * @returns {Promise}
     */
    send: async ({ topic, messages, acks, timeout, compression }) => {
      return retrier(async (bail, retryCount, retryTime) => {
        try {
          return sendMessages({
            topic,
            messages,
            acks,
            timeout,
            compression,
          })
        } catch (error) {
          // This is necessary in case the metadata is stale and the number of partitions
          // for this topic has increased in the meantime
          if (error instanceof KafkaProtocolError) {
            logger.error(`Failed to send messages: ${e.message}`, { retryCount, retryTime })
            await cluster.refreshMetadata()
            throw error
          }

          // Skip retries for errors not related to the Kafka protocol
          bail(error)
        }
      })
    },
  }
}
