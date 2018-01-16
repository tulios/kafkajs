const Long = require('long')
const createLagBasedAssigner = require('./assigners/lagBasedAssigner')
const ConsumerGroup = require('./consumerGroup')
const Runner = require('./runner')
const events = require('./instrumentationEvents')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { KafkaJSNonRetriableError } = require('../errors')

const { keys, values } = Object

const eventNames = values(events)
const eventKeys = keys(events)
  .map(key => `consumer.events.${key}`)
  .join(', ')

module.exports = ({
  cluster,
  groupId,
  logger: rootLogger,
  createPartitionAssigner = createLagBasedAssigner,
  sessionTimeout = 30000,
  heartbeatInterval = 3000,
  maxBytesPerPartition = 1048576, // 1MB
  minBytes = 1,
  maxBytes = 10485760, // 10MB
  maxWaitTimeInMs = 5000,
  retry = {
    retries: 10,
  },
}) => {
  const topics = {}
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const logger = rootLogger.namespace('Consumer')
  const assigner = createPartitionAssigner({
    cluster,
    groupId,
    logger,
  })

  let runner = null
  let consumerGroup = null

  const createConsumerGroup = () => {
    return new ConsumerGroup({
      logger: rootLogger,
      topics: keys(topics),
      topicConfigurations: topics,
      cluster,
      groupId,
      assigner,
      sessionTimeout,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      instrumentationEmitter,
    })
  }

  const createRunner = ({ eachBatch, eachMessage, onCrash }) => {
    return new Runner({
      logger: rootLogger,
      consumerGroup,
      eachBatch,
      eachMessage,
      heartbeatInterval,
      retry,
      onCrash,
    })
  }

  /**
   * @returns {Promise}
   */
  const connect = async () => await cluster.connect()

  /**
   * @return {Promise}
   */
  const disconnect = async () => {
    try {
      if (runner) {
        await runner.stop()
        logger.debug('consumer has stopped, disconnecting', { groupId })
      }
      await cluster.disconnect()
    } catch (e) {}
    logger.info('Stopped', { groupId })
  }

  /**
   * @param {string} topic
   * @param {string} [fromBeginning=false]
   * @return {Promise}
   */
  const subscribe = async ({ topic, fromBeginning = false }) => {
    topics[topic] = { fromBeginning }
    await cluster.addTargetTopic(topic)
  }

  /**
   * @param {Function} [eachBatch=null]
   * @param {Function} [eachMessage=null]
   * @return {Promise}
   */
  const run = async ({ eachBatch = null, eachMessage = null } = {}) => {
    consumerGroup = createConsumerGroup()

    const start = async onCrash => {
      logger.info('Starting', { groupId })
      runner = createRunner({ eachBatch, eachMessage, onCrash })
      await runner.start()
    }

    const restart = onCrash => {
      consumerGroup = createConsumerGroup()
      start(onCrash)
    }

    const onCrash = async e => {
      logger.error(`Crash: ${e.name}: ${e.message}`, { retryCount: e.retryCount, groupId })
      await disconnect()

      if (e.name === 'KafkaJSNumberOfRetriesExceeded') {
        logger.error(`Restarting the consumer in ${e.retryTime}ms`, {
          retryCount: e.retryCount,
          groupId,
        })
        setTimeout(() => restart(onCrash), e.retryTime)
      }
    }

    await start(onCrash)
  }

  /**
   * @param {string} eventName
   * @param {Function} listener
   * @return {Function}
   */
  const on = (eventName, listener) => {
    if (!eventNames.includes(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }

    return instrumentationEmitter.addListener(eventName, event => listener(event))
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @param {string} offset
   */
  const seek = ({ topic, partition, offset }) => {
    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }
    if (isNaN(partition)) {
      throw new KafkaJSNonRetriableError(
        `Invalid partition, expected a number received ${partition}`
      )
    }
    try {
      if (Long.fromValue(offset).lessThan(0)) {
        throw new KafkaJSNonRetriableError('Offset must not be a negative number')
      }
    } catch (_) {
      throw new KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset}`)
    }
    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.seek({ topic, partition, offset })
  }

  return {
    connect,
    disconnect,
    subscribe,
    run,
    seek,
    on,
    events,
  }
}
