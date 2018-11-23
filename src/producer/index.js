const createRetry = require('../retry')
const createDefaultPartitioner = require('./partitioners/default')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const events = require('./instrumentationEvents')
const createTransactionManager = require('./transactionManager')
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
  const nontransactionalTransactionManager = createTransactionManager({
    logger,
    cluster,
    transactionTimeout,
    transactional: false,
    transactionalId,
  })
  let transactionManager

  const { send, sendBatch } = createMessageProducer({
    logger,
    cluster,
    partitioner,
    transactionManager: nontransactionalTransactionManager,
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

  const transaction = async () => {
    if (!transactionalId) {
      throw new KafkaJSNonRetriableError('Must provide transactional id for transactional producer')
    }

    // We want to only initialize the (transactional) transaction manager once
    transactionManager =
      transactionManager ||
      createTransactionManager({
        logger,
        cluster,
        transactionTimeout,
        transactional: true,
        transactionalId,
      })

    if (!transactionManager.isInitialized()) {
      await transactionManager.initProducerId()
    }
    transactionManager.beginTransaction()

    const { send: sendTxn, sendBatch: sendBatchTxn } = createMessageProducer({
      logger,
      cluster,
      partitioner,
      retrier,
      transactionManager,
      idempotent: true,
    })

    // Should we throw an error when calling this methods after transaction has ended?
    return {
      sendBatchTxn,
      sendTxn,
      /**
       * Abort the ongoing transaction.
       *
       * @throws {KafkaJSNonRetriableError} If non-transactional
       */
      abort: () => {
        return transactionManager.abort()
      },
      /**
       * Commit the ongoing transaction.
       *
       * @throws {KafkaJSNonRetriableError} If non-transactional
       */
      commit: () => {
        return transactionManager.commit()
      },
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

      if (idempotent && !nontransactionalTransactionManager.isInitialized()) {
        await nontransactionalTransactionManager.initProducerId()
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
