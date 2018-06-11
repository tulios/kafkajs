const createRetry = require('../retry')
const { KafkaJSNonRetriableError } = require('../errors')

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
   * @return {Promise}
   */
  const createTopics = async ({ topics, validateOnly, timeout }) => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
    }

    if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, the topic names have to be a valid string'
      )
    }

    const set = new Set(topics.map(({ topic }) => topic))
    if (set.size < topics.length) {
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
        return true
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not create topics', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  return {
    connect,
    disconnect,
    createTopics,
  }
}
