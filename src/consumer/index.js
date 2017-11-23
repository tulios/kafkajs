const createRoundRobinAssigned = require('./assigners/roundRobinAssigner')
const ConsumerGroup = require('./consumerGroup')
const Runner = require('./runner')

module.exports = ({
  cluster,
  groupId,
  logger: rootLogger,
  createPartitionAssigner = createRoundRobinAssigned,
  sessionTimeout = 30000,
  heartbeatInterval = 10000,
  maxBytesPerPartition = 1048576, // 1MB
  minBytes = 1,
  maxBytes = 10485760, // 10MB
  maxWaitTimeInMs = 5000,
  retry = {
    retries: 10,
  },
}) => {
  const assigner = createPartitionAssigner({ cluster })
  const logger = rootLogger.namespace('Consumer')
  const topics = {}
  let runner = null

  const createRunner = ({ eachBatch, eachMessage, onCrash }) => {
    const consumerGroup = new ConsumerGroup({
      logger: rootLogger,
      topics: Object.keys(topics),
      topicConfigurations: topics,
      cluster,
      groupId,
      assigner,
      sessionTimeout,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
    })

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
        logger.debug('consumer has stopped, disconnecting')
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
    const start = async onCrash => {
      logger.info('Starting', { groupId })
      runner = createRunner({ eachBatch, eachMessage, onCrash })
      await runner.start()
    }

    const onCrash = async e => {
      logger.error(`Crash: ${e.name}: ${e.message}`, { retryCount: e.retryCount })
      await disconnect()

      if (e.name === 'KafkaJSNumberOfRetriesExceeded') {
        logger.error(`Restarting the consumer in ${e.retryTime}ms`)
        setTimeout(() => start(onCrash), e.retryTime)
      }
    }

    await start(onCrash)
  }

  return {
    connect,
    disconnect,
    subscribe,
    run,
  }
}
