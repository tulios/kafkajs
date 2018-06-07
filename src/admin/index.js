const { KafkaJSNonRetriableError } = require('../errors')

module.exports = ({ logger: rootLogger, cluster }) => {
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

    const broker = await cluster.pickOneBroker()
    return broker.createTopics({ topics, validateOnly, timeout })
  }

  return {
    connect,
    disconnect,
    createTopics,
  }
}
