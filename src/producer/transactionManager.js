const NO_PRODUCER_ID = -1
const SEQUENCE_START = 0

/**
 * Manage behavior for an idempotent producer and transactions.
 */
module.exports = ({ logger, cluster }) => {
  /**
   * Current producer ID
   */
  let producerId = NO_PRODUCER_ID
  /**
   * Current producer epoch
   */
  let producerEpoch = 0
  /**
   * Idempotent production requires that the producer track the sequence number of messages.
   *
   * Sequences are sent with every Record Batch and tracked per Topic-Partition
   */
  let producerSequence = {}

  const isInitialized = () => producerId !== NO_PRODUCER_ID

  return {
    /**
     * Initialize the idempotent producer by making an `InitProducerId` request.
     *
     * Overwrites any existing state in this transaction manager
     */
    initProducerId: async () => {
      await cluster.refreshMetadata()
      // If non-transactional we can request the PID from any broker
      const broker = await cluster.findControllerBroker()
      const result = await broker.initProducerId({
        transactionTimeout: 30000,
      })

      producerId = result.producerId
      producerEpoch = result.producerEpoch
      producerSequence = {}

      logger.debug('Initialized producer id & epoch', { producerId, producerEpoch })
    },

    /**
     * Get the current sequence for a given Topic-Partition. Defaults to 0.
     * @param {string} topic
     * @param {string} partition
     * @returns {number}
     */
    getSequence(topic, partition) {
      if (!isInitialized()) return SEQUENCE_START

      producerSequence[topic] = producerSequence[topic] || {}
      producerSequence[topic][partition] = producerSequence[topic][partition] || SEQUENCE_START

      return producerSequence[topic][partition]
    },

    /**
     * Update the sequence for a given Topic-Partition.
     *
     * Do nothing if not yet initialized (not idempotent)
     * @param {string} topic
     * @param {string} partition
     * @param {number} sequence
     */
    updateSequence(topic, partition, sequence) {
      if (!isInitialized()) return

      producerSequence[topic] = producerSequence[topic] || {}
      producerSequence[topic][partition] = sequence
    },

    /**
     * Get the current producer id
     * @returns {number}
     */
    getProducerId() {
      return producerId
    },

    /**
     * Get the current producer epoch
     * @returns {number}
     */
    getProducerEpoch() {
      return producerEpoch
    },
  }
}
