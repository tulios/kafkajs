const Long = require('long')
const createRetry = require('../retry')
const { initialRetryTime } = require('../retry/defaults')
const ConsumerGroup = require('./consumerGroup')
const Runner = require('./runner')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { KafkaJSNonRetriableError } = require('../errors')
const { roundRobin } = require('./assigners')
const { EARLIEST_OFFSET, LATEST_OFFSET } = require('../constants')
const ISOLATION_LEVEL = require('../protocol/isolationLevel')

const { keys, values } = Object
const { CONNECT, DISCONNECT, STOP, CRASH } = events

const eventNames = values(events)
const eventKeys = keys(events)
  .map(key => `consumer.events.${key}`)
  .join(', ')

const specialOffsets = [
  Long.fromValue(EARLIEST_OFFSET).toString(),
  Long.fromValue(LATEST_OFFSET).toString(),
]

module.exports = ({
  cluster,
  groupId,
  logger: rootLogger,
  partitionAssigners = [roundRobin],
  sessionTimeout = 30000,
  rebalanceTimeout = 60000,
  heartbeatInterval = 3000,
  maxBytesPerPartition = 1048576, // 1MB
  minBytes = 1,
  maxBytes = 10485760, // 10MB
  maxWaitTimeInMs = 5000,
  retry = { retries: 10 },
  isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
  instrumentationEmitter: rootInstrumentationEmitter,
}) => {
  if (!groupId) {
    throw new KafkaJSNonRetriableError('Consumer groupId must be a non-empty string.')
  }

  const logger = rootLogger.namespace('Consumer')
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()
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
      rebalanceTimeout,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      instrumentationEmitter,
      autoCommitInterval,
      autoCommitThreshold,
      isolationLevel,
    })
  }

  const createRunner = ({
    eachBatchAutoResolve,
    eachBatch,
    eachMessage,
    onCrash,
    autoCommit,
    partitionsConsumedConcurrently,
  }) => {
    return new Runner({
      autoCommit,
      logger: rootLogger,
      consumerGroup,
      instrumentationEmitter,
      eachBatchAutoResolve,
      eachBatch,
      eachMessage,
      heartbeatInterval,
      retry,
      onCrash,
      partitionsConsumedConcurrently,
    })
  }

  /**
   * @returns {Promise}
   */
  const connect = async () => {
    await cluster.connect()
    instrumentationEmitter.emit(CONNECT)
  }

  /**
   * @return {Promise}
   */
  const disconnect = async () => {
    try {
      await stop()
      logger.debug('consumer has stopped, disconnecting', { groupId })
      await cluster.disconnect()
      instrumentationEmitter.emit(DISCONNECT)
    } catch (e) {}
  }

  /**
   * @return {Promise}
   */
  const stop = async () => {
    try {
      if (runner) {
        await runner.stop()
        runner = null
        consumerGroup = null
        instrumentationEmitter.emit(STOP)
      }

      logger.info('Stopped', { groupId })
    } catch (e) {}
  }

  /**
   * @param {string | RegExp} topic
   * @param {boolean} [fromBeginning=false]
   * @return {Promise}
   */
  const subscribe = async ({ topic, fromBeginning = false }) => {
    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const isRegExp = topic instanceof RegExp
    if (typeof topic !== 'string' && !isRegExp) {
      throw new KafkaJSNonRetriableError(
        `Invalid topic ${topic} (${typeof topic}), the topic name has to be a String or a RegExp`
      )
    }

    const topicsToSubscribe = []
    if (isRegExp) {
      const topicRegExp = topic
      const metadata = await cluster.metadata()
      const matchedTopics = metadata.topicMetadata
        .map(({ topic: topicName }) => topicName)
        .filter(topicName => topicRegExp.test(topicName))

      logger.debug('Subscription based on RegExp', {
        groupId,
        topicRegExp: topicRegExp.toString(),
        matchedTopics,
      })

      topicsToSubscribe.push(...matchedTopics)
    } else {
      topicsToSubscribe.push(topic)
    }

    for (const t of topicsToSubscribe) {
      topics[t] = { fromBeginning }
    }

    await cluster.addMultipleTargetTopics(topicsToSubscribe)
  }

  /**
   * @param {boolean} [autoCommit=true]
   * @param {number} [autoCommitInterval=null]
   * @param {number} [autoCommitThreshold=null]
   * @param {boolean} [eachBatchAutoResolve=true] Automatically resolves the last offset of the batch when the
   *                                              the callback succeeds
   * @param {number} [partitionsConsumedConcurrently=1]
   * @param {Function} [eachBatch=null]
   * @param {Function} [eachMessage=null]
   * @return {Promise}
   */
  const run = async ({
    autoCommit = true,
    autoCommitInterval = null,
    autoCommitThreshold = null,
    eachBatchAutoResolve = true,
    partitionsConsumedConcurrently = 1,
    eachBatch = null,
    eachMessage = null,
  } = {}) => {
    if (consumerGroup) {
      logger.warn('consumer#run was called, but the consumer is already running', { groupId })
      return
    }

    consumerGroup = createConsumerGroup({
      autoCommitInterval,
      autoCommitThreshold,
    })

    const start = async onCrash => {
      logger.info('Starting', { groupId })
      runner = createRunner({
        autoCommit,
        eachBatchAutoResolve,
        eachBatch,
        eachMessage,
        onCrash,
        partitionsConsumedConcurrently,
      })

      await runner.start()
    }

    const restart = onCrash => {
      consumerGroup = createConsumerGroup({
        autoCommit,
        autoCommitInterval,
        autoCommitThreshold,
      })

      start(onCrash)
    }

    const onCrash = async e => {
      logger.error(`Crash: ${e.name}: ${e.message}`, {
        groupId,
        retryCount: e.retryCount,
        stack: e.stack,
      })

      await disconnect()

      instrumentationEmitter.emit(CRASH, {
        error: e,
        groupId,
      })

      if (e.name === 'KafkaJSNumberOfRetriesExceeded' || e.retriable === true) {
        const retryTime = e.retryTime || retry.initialRetryTime || initialRetryTime
        logger.error(`Restarting the consumer in ${retryTime}ms`, {
          retryCount: e.retryCount,
          retryTime,
          groupId,
        })

        setTimeout(() => restart(onCrash), retryTime)
      }
    }

    await start(onCrash)
  }

  /**
   * @param {string} eventName
   * @param {AsyncFunction} listener
   * @return {Function} removeListener
   */
  const on = (eventName, listener) => {
    if (!eventNames.includes(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }

    return instrumentationEmitter.addListener(unwrapEvent(eventName), event => {
      event.type = wrapEvent(event.type)
      Promise.resolve(listener(event)).catch(e => {
        logger.error(`Failed to execute listener: ${e.message}`, {
          eventName,
          stack: e.stack,
        })
      })
    })
  }

  /**
   * @param {Array<TopicPartitionOffsetAndMetadata>} topicPartitions
   *   Example: [{ topic: 'topic-name', partition: 0, offset: '1', metadata: 'event-id-3' }]
   */
  const commitOffsets = async (topicPartitions = []) => {
    const commitsByTopic = topicPartitions.reduce(
      (payload, { topic, partition, offset, metadata = null }) => {
        if (!topic) {
          throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
        }

        if (isNaN(partition)) {
          throw new KafkaJSNonRetriableError(
            `Invalid partition, expected a number received ${partition}`
          )
        }

        let commitOffset
        try {
          commitOffset = Long.fromValue(offset)
        } catch (_) {
          throw new KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset}`)
        }

        if (commitOffset.lessThan(0)) {
          throw new KafkaJSNonRetriableError('Offset must not be a negative number')
        }

        if (metadata !== null && typeof metadata !== 'string') {
          throw new KafkaJSNonRetriableError(
            `Invalid offset metatadta, expected string or null, received ${metadata}`
          )
        }

        const topicCommits = payload[topic] || []

        topicCommits.push({ partition, offset: commitOffset, metadata })

        return { ...payload, [topic]: topicCommits }
      },
      {}
    )

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    const topics = Object.keys(commitsByTopic)

    return runner.commitOffsets({
      topics: topics.map(topic => {
        return {
          topic,
          partitions: commitsByTopic[topic],
        }
      }),
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

    let seekOffset
    try {
      seekOffset = Long.fromValue(offset)
    } catch (_) {
      throw new KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset}`)
    }

    if (seekOffset.lessThan(0) && !specialOffsets.includes(seekOffset.toString())) {
      throw new KafkaJSNonRetriableError('Offset must not be a negative number')
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.seek({ topic, partition, offset: seekOffset.toString() })
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
   * @param {Array<TopicPartitions>} topicPartitions
   *   Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   *
   * @typedef {Object} TopicPartitions
   * @property {string} topic
   * @property {Array<{number}>} [partitions]
   */
  const pause = (topicPartitions = []) => {
    for (const topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${(topicPartition && topicPartition.topic) || topicPartition}`
        )
      } else if (
        typeof topicPartition.partitions !== 'undefined' &&
        (!Array.isArray(topicPartition.partitions) || topicPartition.partitions.some(isNaN))
      ) {
        throw new KafkaJSNonRetriableError(
          `Array of valid partitions required to pause specific partitions instead of ${topicPartition.partitions}`
        )
      }
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.pause(topicPartitions)
  }

  /**
   * Returns the list of topic partitions paused on this consumer
   *
   * @returns {Array<TopicPartitions>}
   */
  const paused = () => {
    if (!consumerGroup) {
      return []
    }

    return consumerGroup.paused()
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions
   *  Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   *
   * @typedef {Object} TopicPartitions
   * @property {string} topic
   * @property {Array<{number}>} [partitions]
   */
  const resume = (topicPartitions = []) => {
    for (const topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${(topicPartition && topicPartition.topic) || topicPartition}`
        )
      } else if (
        typeof topicPartition.partitions !== 'undefined' &&
        (!Array.isArray(topicPartition.partitions) || topicPartition.partitions.some(isNaN))
      ) {
        throw new KafkaJSNonRetriableError(
          `Array of valid partitions required to resume specific partitions instead of ${topicPartition.partitions}`
        )
      }
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.resume(topicPartitions)
  }

  /**
   * @return {Object} logger
   */
  const getLogger = () => logger

  return {
    connect,
    disconnect,
    subscribe,
    stop,
    run,
    commitOffsets,
    seek,
    describeGroup,
    pause,
    paused,
    resume,
    on,
    events,
    logger: getLogger,
  }
}
