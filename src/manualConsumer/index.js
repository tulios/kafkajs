const Long = require('../utils/long')
const { initialRetryTime } = require('../retry/defaults')
const ManualConsumer = require('./manualConsumer')
const Runner = require('./runner')
const {
  events,
  wrap: wrapEvent,
  unwrap: unwrapEvent,
} = require('../consumer/instrumentationEvents')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { KafkaJSNonRetriableError } = require('../errors')
const { EARLIEST_OFFSET, LATEST_OFFSET } = require('../constants')
const ISOLATION_LEVEL = require('../protocol/isolationLevel')
const sharedPromiseTo = require('../utils/sharedPromiseTo')

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

/**
 * @param {Object} params
 * @param {import("../../types").Cluster} params.cluster
 * @param {import('../../types').RetryOptions} [params.retry]
 * @param {import('../../types').Logger} params.logger
 * @param {import('../../types').PartitionAssigner[]} [params.partitionAssigners]
 * @param {number} [params.maxBytesPerPartition]
 * @param {number} [params.minBytes]
 * @param {number} [params.maxBytes]
 * @param {number} [params.maxWaitTimeInMs]
 * @param {number} [params.isolationLevel]
 * @param {string} [params.rackId]
 * @param {InstrumentationEventEmitter} [params.instrumentationEmitter]
 * @param {number} params.metadataMaxAge
 *
 * @returns {import("../../types").ManualConsumer}
 */
module.exports = ({
  cluster,
  retry,
  logger: rootLogger,
  maxBytesPerPartition = 1048576, // 1MB
  minBytes = 1,
  maxBytes = 10485760, // 10MB
  maxWaitTimeInMs = 5000,
  isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
  rackId = '',
  instrumentationEmitter: rootInstrumentationEmitter,
  metadataMaxAge,
}) => {
  const logger = rootLogger.namespace('ManualConsumer')
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()

  /** @type {Record<string, { fromBeginning?: boolean }>} */
  const topics = {}
  let runner = null
  let manualConsumer = null
  let restartTimeout = null

  /** @type {import("../../types").ManualConsumer["connect"]} */
  const connect = async () => {
    await cluster.connect()
    instrumentationEmitter.emit(CONNECT)
  }

  /** @type {import("../../types").ManualConsumer["disconnect"]} */
  const disconnect = async () => {
    try {
      await stop()
      logger.debug('consumer has stopped, disconnecting')
      await cluster.disconnect()
      instrumentationEmitter.emit(DISCONNECT)
    } catch (e) {
      logger.error(`Caught error when disconnecting the consumer: ${e.message}`, {
        stack: e.stack,
      })
      throw e
    }
  }

  /** @type {import("../../types").ManualConsumer["stop"]} */
  const stop = sharedPromiseTo(async () => {
    try {
      if (runner) {
        await runner.stop()
        runner = null
        manualConsumer = null
        instrumentationEmitter.emit(STOP)
      }

      clearTimeout(restartTimeout)
      logger.info('Stopped')
    } catch (e) {
      logger.error(`Caught error when stopping the consumer: ${e.message}`, {
        stack: e.stack,
      })

      throw e
    }
  })

  /** @type {import("../../types").ManualConsumer["subscribe"]} */
  const subscribe = async ({ topic, topics: subscriptionTopics, fromBeginning = false }) => {
    if (manualConsumer) {
      throw new KafkaJSNonRetriableError('Cannot subscribe to topic while consumer is running')
    }

    if (!topic && !subscriptionTopics) {
      throw new KafkaJSNonRetriableError('Missing required argument "topics"')
    }

    if (subscriptionTopics != null && !Array.isArray(subscriptionTopics)) {
      throw new KafkaJSNonRetriableError('Argument "topics" must be an array')
    }

    const subscriptions = subscriptionTopics || [topic]

    for (const subscription of subscriptions) {
      if (typeof subscription !== 'string' && !(subscription instanceof RegExp)) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${subscription} (${typeof subscription}), the topic name has to be a String or a RegExp`
        )
      }
    }

    const hasRegexSubscriptions = subscriptions.some(subscription => subscription instanceof RegExp)
    const metadata = hasRegexSubscriptions ? await cluster.metadata() : undefined

    const topicsToSubscribe = []
    for (const subscription of subscriptions) {
      const isRegExp = subscription instanceof RegExp
      if (isRegExp) {
        const topicRegExp = subscription
        const matchedTopics = metadata.topicMetadata
          .map(({ topic: topicName }) => topicName)
          .filter(topicName => topicRegExp.test(topicName))

        logger.debug('Subscription based on RegExp', {
          topicRegExp: topicRegExp.toString(),
          matchedTopics,
        })

        topicsToSubscribe.push(...matchedTopics)
      } else {
        topicsToSubscribe.push(subscription)
      }
    }

    for (const t of topicsToSubscribe) {
      topics[t] = { fromBeginning }
    }

    await cluster.addMultipleTargetTopics(topicsToSubscribe)
  }

  /** @type {import("../../types").ManualConsumer["run"]} */
  const run = async ({
    eachBatchAutoResolve = true,
    partitionsConsumedConcurrently: concurrency = 1,
    eachBatch = null,
    eachMessage = null,
  } = {}) => {
    if (manualConsumer) {
      logger.warn('consumer#run was called, but the consumer is already running')
      return
    }

    const start = async onCrash => {
      logger.info('Starting')

      manualConsumer = new ManualConsumer({
        logger: rootLogger,
        topics: keys(topics),
        topicConfigurations: topics,
        retry,
        cluster,
        maxBytesPerPartition,
        minBytes,
        maxBytes,
        maxWaitTimeInMs,
        instrumentationEmitter,
        isolationLevel,
        rackId,
        metadataMaxAge,
      })

      runner = new Runner({
        logger: rootLogger,
        manualConsumer,
        instrumentationEmitter,
        retry,
        eachBatchAutoResolve,
        eachBatch,
        eachMessage,
        onCrash,
        concurrency,
      })

      await runner.start()
    }

    const onCrash = async e => {
      logger.error(`Crash: ${e.name}: ${e.message}`, {
        retryCount: e.retryCount,
        stack: e.stack,
      })

      if (e.name === 'KafkaJSConnectionClosedError') {
        cluster.removeBroker({ host: e.host, port: e.port })
      }

      await disconnect()

      const getOriginalCause = error => {
        if (error.cause) {
          return getOriginalCause(error.cause)
        }

        return error
      }

      const isErrorRetriable =
        e.name === 'KafkaJSNumberOfRetriesExceeded' || getOriginalCause(e).retriable === true
      const shouldRestart =
        isErrorRetriable &&
        (!retry ||
          !retry.restartOnFailure ||
          (await retry.restartOnFailure(e).catch(error => {
            logger.error(
              'Caught error when invoking user-provided "restartOnFailure" callback. Defaulting to restarting.',
              {
                error: error.message || error,
                cause: e.message || e,
              }
            )

            return true
          })))

      instrumentationEmitter.emit(CRASH, {
        error: e,
        restart: shouldRestart,
      })

      if (shouldRestart) {
        const retryTime = e.retryTime || (retry && retry.initialRetryTime) || initialRetryTime
        logger.error(`Restarting the consumer in ${retryTime}ms`, {
          retryCount: e.retryCount,
          retryTime,
        })

        restartTimeout = setTimeout(() => start(onCrash), retryTime)
      }
    }

    await start(onCrash)
  }

  /** @type {import("../../types").ManualConsumer["on"]} */
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

  /** @type {import("../../types").ManualConsumer["seek"]} */
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

    if (!manualConsumer) {
      throw new KafkaJSNonRetriableError(
        'Consumer was not initialized, consumer#run must be called first'
      )
    }

    manualConsumer.seek({ topic, partition, offset: seekOffset.toString() })
  }

  /**
   * @type {import("../../types").ManualConsumer["pause"]}
   * @param topicPartitions
   *   Example: [{ topic: 'topic-name', partitions: [1, 2] }]
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

    if (!manualConsumer) {
      throw new KafkaJSNonRetriableError(
        'Consumer was not initialized, consumer#run must be called first'
      )
    }

    manualConsumer.pause(topicPartitions)
  }

  /**
   * Returns the list of topic partitions paused on this manualConsumer
   *
   * @type {import("../../types").ManualConsumer["paused"]}
   */
  const paused = () => {
    if (!manualConsumer) {
      return []
    }

    return manualConsumer.paused()
  }

  /**
   * @type {import("../../types").ManualConsumer["resume"]}
   * @param topicPartitions
   *  Example: [{ topic: 'topic-name', partitions: [1, 2] }]
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

    if (!manualConsumer) {
      throw new KafkaJSNonRetriableError(
        'Consumer was not initialized, consumer#run must be called first'
      )
    }

    manualConsumer.resume(topicPartitions)
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
    seek,
    pause,
    paused,
    resume,
    on,
    events,
    logger: getLogger,
  }
}
