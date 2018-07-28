const Long = require('long')
const createRetry = require('../retry')
const ConsumerGroup = require('./consumerGroup')
const Runner = require('./runner')
const events = require('./instrumentationEvents')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { KafkaJSNonRetriableError } = require('../errors')
const { roundRobin } = require('./assigners')

const { keys, values } = Object

const eventNames = values(events)
const eventKeys = keys(events)
  .map(key => `consumer.events.${key}`)
  .join(', ')

module.exports = ({
  cluster,
  groupId,
  logger: rootLogger,
  partitionAssigners = [roundRobin],
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
  const logger = rootLogger.namespace('Consumer')
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const assigners = partitionAssigners.map(createAssigner =>
    createAssigner({ groupId, logger, cluster })
  )

  const topics = {}
  let runner = null
  let consumerGroup = null

  if (heartbeatInterval >= sessionTimeout) {
    throw new KafkaJSNonRetriableError(
      `Consumer heartbeatInterval (${heartbeatInterval}) must be lower than sessionTimeout (${sessionTimeout}). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.`
    )
  }

  const createConsumerGroup = ({ autoCommitInterval, autoCommitThreshold }) => {
    return new ConsumerGroup({
      logger: rootLogger,
      topics: keys(topics),
      topicConfigurations: topics,
      cluster,
      groupId,
      assigners,
      sessionTimeout,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      instrumentationEmitter,
      autoCommitInterval,
      autoCommitThreshold,
    })
  }

  const createRunner = ({ eachBatchAutoResolve, eachBatch, eachMessage, onCrash }) => {
    return new Runner({
      logger: rootLogger,
      consumerGroup,
      instrumentationEmitter,
      eachBatchAutoResolve,
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
    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    topics[topic] = { fromBeginning }
    await cluster.addTargetTopic(topic)
  }

  /**
   * @param {number} [autoCommitInterval=null]
   * @param {number} [autoCommitThreshold=null]
   * @param {boolean} [eachBatchAutoResolve=true] Automatically resolves the last offset of the batch when the
   *                                              the callback succeeds
   * @param {Function} [eachBatch=null]
   * @param {Function} [eachMessage=null]
   * @return {Promise}
   */
  const run = async (
    {
      autoCommitInterval = null,
      autoCommitThreshold = null,
      eachBatchAutoResolve = true,
      eachBatch = null,
      eachMessage = null,
    } = {}
  ) => {
    consumerGroup = createConsumerGroup({
      autoCommitInterval,
      autoCommitThreshold,
    })

    const start = async onCrash => {
      logger.info('Starting', { groupId })
      runner = createRunner({
        eachBatchAutoResolve,
        eachBatch,
        eachMessage,
        onCrash,
      })

      await runner.start()
    }

    const restart = onCrash => {
      consumerGroup = createConsumerGroup({
        autoCommitInterval,
        autoCommitThreshold,
      })

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

  /**
   * @returns Promise<GroupDescription>
   *
   * @typedef {Object} GroupDescription
   * @property {string} groupId
   * @property {Array<MemberDescription>} members
   * @property {string} protocol
   * @property {string} protocolType
   * @property {string} state
   *
   * @typedef {Object} MemberDescription
   * @property {string} clientHost
   * @property {string} clientId
   * @property {string} memberId
   * @property {Buffer} memberAssignment
   * @property {Buffer} memberMetadata
   */
  const describeGroup = async () => {
    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const retrier = createRetry(retry)
    return retrier(async () => {
      const { groups } = await coordinator.describeGroups({ groupIds: [groupId] })
      return groups.find(group => group.groupId === groupId)
    })
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   *
   * @typedef {Object} TopicPartitions
   * @property {string} topic
   * @property {Array<{number}>} [partitions] Not used at the moment. The entire topic will be paused,
   *                                          regardless of the partitions passed in
   */
  const pause = (topicPartitions = []) => {
    for (let topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${(topicPartition && topicPartition.topic) || topicPartition}`
        )
      }
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.pause(topicPartitions.map(({ topic }) => topic))
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   *
   * @typedef {Object} TopicPartitions
   * @property {string} topic
   * @property {Array<{number}>} [partitions] Not used at the moment. All partitions will be consumed regardless
   *                                          of the partitions passed in.
   */
  const resume = (topicPartitions = []) => {
    for (let topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${(topicPartition && topicPartition.topic) || topicPartition}`
        )
      }
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.resume(topicPartitions.map(({ topic }) => topic))
  }

  return {
    connect,
    disconnect,
    subscribe,
    run,
    seek,
    describeGroup,
    pause,
    resume,
    on,
    events,
  }
}
