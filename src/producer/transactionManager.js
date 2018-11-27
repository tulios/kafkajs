const { INT_32_MAX_VALUE } = require('../constants')

const NO_PRODUCER_ID = -1
const SEQUENCE_START = 0

/**
 * Manage behavior for an idempotent producer and transactions.
 */
module.exports = ({ logger, cluster, transactionTimeout = 60000 }) => {
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

  const transactionManager = {
    isInitialized() {
      return producerId !== NO_PRODUCER_ID
    },

    /**
     * Initialize the idempotent producer by making an `InitProducerId` request.
     * Overwrites any existing state in this transaction manager
     */
    initProducerId: async () => {
      await cluster.refreshMetadataIfNecessary()

      // If non-transactional we can request the PID from any broker
      const broker = await cluster.findControllerBroker()
      const result = await broker.initProducerId({ transactionTimeout })

      producerId = result.producerId
      producerEpoch = result.producerEpoch
      producerSequence = {}

      logger.debug('Initialized producer id & epoch', { producerId, producerEpoch })
    },

    /**
     * Get the current sequence for a given Topic-Partition. Defaults to 0.
     *
     * @param {string} topic
     * @param {string} partition
     * @returns {number}
     */
    getSequence(topic, partition) {
      if (!transactionManager.isInitialized()) {
        return SEQUENCE_START
      }

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
     * @param {number} increment
     */
    updateSequence(topic, partition, increment) {
      if (!transactionManager.isInitialized()) {
        return
      }

      const previous = transactionManager.getSequence(topic, partition)
      let sequence = previous + increment

      // Sequence is defined as Int32 in the Record Batch,
      // so theoretically should need to rotate here
      if (sequence >= INT_32_MAX_VALUE) {
        logger.debug(
          `Sequence for ${topic} ${partition} exceeds max value (${sequence}). Rotating to 0.`
        )
        sequence = 0
      }

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

  return transactionManager
}
