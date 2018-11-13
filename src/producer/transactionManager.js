const { KafkaJSNonRetriableError } = require('../errors')
const COORDINATOR_TYPES = require('../protocol/coordinatorTypes')

const NO_PRODUCER_ID = -1
const SEQUENCE_START = 0
const INT_32_MAX_VALUE = Math.pow(2, 32)

/**
 * Manage behavior for an idempotent producer and transactions.
 */
module.exports = ({
  logger,
  cluster,
  transactionTimeout = 60000,
  transactional,
  transactionalId,
}) => {
  if (transactional && !transactionalId) {
    throw new KafkaJSNonRetriableError('Cannot manage transactions without a transactionalId')
  }

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
  let inTransaction = false

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
      const broker = transactional
        ? await cluster.findGroupCoordinator({
            groupId: transactionalId,
            coordinatorType: COORDINATOR_TYPES.TRANSACTION,
          })
        : await cluster.findControllerBroker()

      const result = await broker.initProducerId({
        groupId: transactional ? transactionalId : undefined,
        transactionTimeout,
      })

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

    getTransactionalId() {
      return transactionalId
    },

    /**
     * Begin a transaction
     */
    beginTransaction() {
      if (!isInitialized()) {
        throw new KafkaJSNonRetriableError(
          'Cannot begin transaction prior to initializing producer id'
        )
      }

      if (inTransaction) {
        throw new KafkaJSNonRetriableError('Complete transaction before beginning another')
      }

      inTransaction = true
    },

    /**
     * Commit the ongoing transaction
     */
    async commit() {
      if (!inTransaction) {
        throw new KafkaJSNonRetriableError('Cannot commit outside transaction')
      }

      const broker = await cluster.findGroupCoordinator({ groupId: transactionalId })
      await broker.endTxn({ producerId, producerEpoch, transactionalId, transactionalResult: true })

      inTransaction = false
    },

    /**
     * Abort the ongoing transaction
     */
    async abort() {
      if (!inTransaction) {
        throw new KafkaJSNonRetriableError('Cannot commit outside transaction')
      }

      const broker = await cluster.findGroupCoordinator({ groupId: transactionalId })
      await broker.endTxn({
        producerId,
        producerEpoch,
        transactionalId,
        transactionalResult: false,
      })

      inTransaction = false
    },

    isInTransaction() {
      return !!inTransaction
    },
  }

  return transactionManager
}
