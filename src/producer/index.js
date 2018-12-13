const createRetry = require('../retry')
const createDefaultPartitioner = require('./partitioners/default')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const events = require('./instrumentationEvents')
const createEosManager = require('./eosManager')
const createMessageProducer = require('./messageProducer')
const { CONNECT, DISCONNECT } = require('./instrumentationEvents')
const { KafkaJSNonRetriableError } = require('../errors')

const { values, keys } = Object
const eventNames = values(events)
const eventKeys = keys(events)
  .map(key => `producer.events.${key}`)
  .join(', ')

module.exports = ({
  cluster,
  logger: rootLogger,
  createPartitioner = createDefaultPartitioner,
  retry,
  idempotent = false,
  transactionalId,
  transactionTimeout,
}) => {
  retry = retry || { retries: idempotent ? Number.MAX_SAFE_INTEGER : 5 }

  if (idempotent && retry.retries < 1) {
    throw new KafkaJSNonRetriableError(
      'Idempotent producer must allow retries to protect against transient errors'
    )
  }

  const logger = rootLogger.namespace('Producer')

  if (idempotent && retry.retries < Number.MAX_SAFE_INTEGER) {
    logger.warn('Limiting retries for the idempotent producer may invalidate EoS guarantees')
  }

  const partitioner = createPartitioner()
  const retrier = createRetry(Object.assign({}, cluster.retry, retry))
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const idempotentEosManager = createEosManager({
    logger,
    cluster,
    transactionTimeout,
    transactional: false,
    transactionalId,
  })
  let transactionalEosManager

  const { send, sendBatch } = createMessageProducer({
    logger,
    cluster,
    partitioner,
    eosManager: idempotentEosManager,
    idempotent,
    retrier,
  })

  /**
   * @param {string} eventName
   * @param {Function} listener
   * @return {Function}
   */
  const on = (eventName, listener) => {
    if (!eventNames.includes(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }

    return instrumentationEmitter.addListener(eventName, event => {
      Promise.resolve(listener(event)).catch(e => {
        logger.error(`Failed to execute listener: ${e.message}`, {
          eventName,
          stack: e.stack,
        })
      })
    })
  }

  /**
   * Begin a transaction. The returned object contains methods to send messages
   * to the transaction and end the transaction by comitting or aborting.
   *
   * Only messages sent on the transaction object will participate in the transaction.
   *
   * Calling any of the transactional methods after the transaction has ended
   * will raise an exception (use `isActive` to ascertain if ended).
   * @returns {Promise<Transaction>}
   *
   * @typedef {Object} Transaction
   * @property {Function} send  Identical to the producer "send" method
   * @property {Function} sendBatch Identical to the producer "sendBatch" method
   * @property {Function} abort Abort the transaction
   * @property {Function} commit  Commit the transaction
   * @property {Function} isActive  Whether the transaction is active
   */
  const transaction = async () => {
    if (!transactionalId) {
      throw new KafkaJSNonRetriableError('Must provide transactional id for transactional producer')
    }

    let transactionDidEnd = false
    transactionalEosManager =
      transactionalEosManager ||
      createEosManager({
        logger,
        cluster,
        transactionTimeout,
        transactional: true,
        transactionalId,
      })

    if (transactionalEosManager.isInTransaction()) {
      throw new KafkaJSNonRetriableError(
        'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.'
      )
    }

    // We only initialize the producer id once
    if (!transactionalEosManager.isInitialized()) {
      await transactionalEosManager.initProducerId()
    }
    transactionalEosManager.beginTransaction()

    const { send: sendTxn, sendBatch: sendBatchTxn } = createMessageProducer({
      logger,
      cluster,
      partitioner,
      retrier,
      eosManager: transactionalEosManager,
      idempotent: true,
    })

    const isActive = () => transactionalEosManager.isInTransaction() && !transactionDidEnd

    const transactionGuard = fn => (...args) => {
      if (!isActive()) {
        return Promise.reject(
          new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
        )
      }

      return fn(...args)
    }

    return {
      sendBatch: transactionGuard(sendBatchTxn),
      send: transactionGuard(sendTxn),
      /**
       * Abort the ongoing transaction.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       */
      abort: transactionGuard(async () => {
        await transactionalEosManager.abort()
        transactionDidEnd = true
      }),
      /**
       * Commit the ongoing transaction.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       */
      commit: transactionGuard(async () => {
        await transactionalEosManager.commit()
        transactionDidEnd = true
      }),
      /**
       * Sends a list of specified offsets to the consumer group coordinator, and also marks those offsets as part of the current transaction.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       */
      sendOffsets: transactionGuard(({ consumerGroupId, offsets }) => {
        const { topics } = offsets
        transactionalEosManager.sendOffsets({ consumerGroupId, topics })

        for (const topicOffsets of topics) {
          const { topic, partitions } = topicOffsets
          for (const { partition, offset } of partitions) {
            cluster.markOffsetAsCommitted({
              groupId: consumerGroupId,
              topic,
              partition,
              offset,
            })
          }
        }
      }),
      isActive,
    }
  }

  /**
   * @returns {Object} logger
   */
  const getLogger = () => logger

  return {
    /**
     * @returns {Promise}
     */
    connect: async () => {
      await cluster.connect()
      instrumentationEmitter.emit(CONNECT)

      if (idempotent && !idempotentEosManager.isInitialized()) {
        await idempotentEosManager.initProducerId()
      }
    },
    /**
     * @return {Promise}
     */
    disconnect: async () => {
      await cluster.disconnect()
      instrumentationEmitter.emit(DISCONNECT)
    },
    isIdempotent: () => {
      return idempotent
    },
    events,
    on,
    send,
    sendBatch,
    transaction,
    logger: getLogger,
  }
}
