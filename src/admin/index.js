const createRetry = require('../retry')
const waitFor = require('../utils/waitFor')
const { KafkaJSNonRetriableError } = require('../errors')

const retryOnLeaderNotAvailable = (fn, opts = {}) => {
  const callback = async () => {
    try {
      return await fn()
    } catch (e) {
      if (e.type !== 'LEADER_NOT_AVAILABLE') {
        throw e
      }
      return false
    }
  }

  return waitFor(callback, opts)
}

module.exports = ({ retry = { retries: 5 }, logger: rootLogger, cluster }) => {
  const logger = rootLogger.namespace('Admin')

  /**
   * @returns {Promise}
   */
  const connect = async () => await cluster.connect()

  /**
   * @return {Promise}
   */
  const disconnect = async () => await cluster.disconnect()

  /**
   * @param {array} topics
   * @param {boolean} [validateOnly=false]
   * @param {number} [timeout=5000]
   * @param {boolean} [waitForLeaders=true]
   * @return {Promise}
   */
  const createTopics = async ({ topics, validateOnly, timeout, waitForLeaders = true }) => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
    }

    if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, the topic names have to be a valid string'
      )
    }

    const topicNames = new Set(topics.map(({ topic }) => topic))
    if (topicNames.size < topics.length) {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, it cannot have multiple entries for the same topic'
      )
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata()
        const broker = await cluster.findControllerBroker()
        await broker.createTopics({ topics, validateOnly, timeout })

        if (waitForLeaders) {
          const topicNamesArray = Array.from(topicNames.values())
          await retryOnLeaderNotAvailable(async () => await broker.metadata(topicNamesArray), {
            delay: 100,
            timeoutMessage: 'Timed out while waiting for topic leaders',
          })
        }

        return true
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not create topics', { error: e.message, retryCount, retryTime })
          throw e
        }

        if (e.type === 'TOPIC_ALREADY_EXISTS') {
          return false
        }

        bail(e)
      }
    })
  }

  /**
   * @return {Object} logger
   */
  const getLogger = () => logger

  return {
    connect,
    disconnect,
    createTopics,
    logger: getLogger,
  }
}
