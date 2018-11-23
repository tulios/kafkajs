const { KafkaJSNonRetriableError } = require('../../errors')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const createStateMachine = require('./transactionStateMachine')

const STATES = require('./transactionStates')
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

  /**
   * Topic partitions already participating in the transaction
   */
  let transactionTopicPartitions = {}

  const stateMachine = createStateMachine({ logger })
  stateMachine.on('transition', ({ to }) => {
    if (to === STATES.READY) {
      transactionTopicPartitions = {}
    }
  })

  const findTransactionCoordinator = () => {
    return cluster.findGroupCoordinator({
      groupId: transactionalId,
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    })
  }

  const transactionalGuard = () => {
    if (!transactional) {
      throw new KafkaJSNonRetriableError('Method unavailable if non-transactional')
    }
  }

  const eosManager = stateMachine.createGuarded(
    {
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
       * Initialize the idempotent producer by making an `InitProducerId` request.
       * Overwrites any existing state in this transaction manager
       */
      initProducerId: async () => {
        await cluster.refreshMetadataIfNecessary()

        // If non-transactional we can request the PID from any broker
        const broker = await (transactional
          ? findTransactionCoordinator()
          : cluster.findControllerBroker())

        const result = await broker.initProducerId({
          transactionalId: transactional ? transactionalId : undefined,
          transactionTimeout,
        })

        stateMachine.transitionTo(STATES.READY)
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
        if (!eosManager.isInitialized()) {
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
        if (!eosManager.isInitialized()) {
          return
        }

        const previous = eosManager.getSequence(topic, partition)
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
       * Begin a transaction
       */
      beginTransaction() {
        transactionalGuard()
        stateMachine.transitionTo(STATES.TRANSACTING)
      },

      /**
       * Add partitions to a transaction if they are not already marked as participating.
       *
       * Should be called prior to sending any messages during a transaction
       * @param {TopicData[]} topicData
       *
       * @typedef {Object} TopicData
       * @property {string} topic
       * @property {object[]} partitions
       * @property {number} partitions[].partition
       */
      async addPartitionsToTransaction(topicData) {
        transactionalGuard()
        const newTopicPartitions = {}

        topicData.forEach(({ topic, partitions }) => {
          transactionTopicPartitions[topic] = transactionTopicPartitions[topic] || {}

          partitions.forEach(({ partition }) => {
            if (!transactionTopicPartitions[topic][partition]) {
              newTopicPartitions[topic] = newTopicPartitions[topic] || []
              newTopicPartitions[topic].push(partition)
            }
          })
        })

        const topics = Object.keys(newTopicPartitions).map(topic => ({
          topic,
          partitions: newTopicPartitions[topic],
        }))

        if (topics.length) {
          const broker = await findTransactionCoordinator()
          await broker.addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics })
        }

        topics.forEach(({ topic, partitions }) => {
          partitions.forEach(partition => {
            transactionTopicPartitions[topic][partition] = true
          })
        })
      },

      /**
       * Commit the ongoing transaction
       */
      async commit() {
        transactionalGuard()
        stateMachine.transitionTo(STATES.COMMITTING)

        const broker = await findTransactionCoordinator()
        await broker.endTxn({
          producerId,
          producerEpoch,
          transactionalId,
          transactionalResult: true,
        })

        stateMachine.transitionTo(STATES.READY)
      },

      /**
       * Abort the ongoing transaction
       */
      async abort() {
        transactionalGuard()
        stateMachine.transitionTo(STATES.ABORTING)

        const broker = await findTransactionCoordinator()
        await broker.endTxn({
          producerId,
          producerEpoch,
          transactionalId,
          transactionalResult: false,
        })

        stateMachine.transitionTo(STATES.READY)
      },

      /**
       * Whether the producer id has already been initialized
       */
      isInitialized() {
        return producerId !== NO_PRODUCER_ID
      },

      isTransactional() {
        return transactional
      },

      isInTransaction() {
        return stateMachine.state() === STATES.TRANSACTING
      },
    },
    /**
     * Transaction state guards
     */
    {
      initProducerId: { legalStates: [STATES.UNINITIALIZED, STATES.READY] },
      beginTransaction: { legalStates: [STATES.READY], async: false },
      addPartitionsToTransaction: { legalStates: [STATES.TRANSACTING] },
      commit: { legalStates: [STATES.TRANSACTING] },
      abort: { legalStates: [STATES.TRANSACTING] },
    }
  )

  return eosManager
}
