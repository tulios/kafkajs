const NO_PRODUCER_ID = -1
const NO_PRODUCER_EPOCH = 0
const SEQUENCE_START = 0

module.exports = ({ logger, cluster }) => {
  let producerId = NO_PRODUCER_ID
  let producerEpoch = NO_PRODUCER_EPOCH
  let producerSequence = {} // Track sequence by topic-partition

  const isInitialized = () => producerId !== NO_PRODUCER_ID || producerEpoch !== NO_PRODUCER_EPOCH

  return {
    getProducerId() {
      return producerId
    },

    getProducerEpoch() {
      return producerEpoch
    },

    getSequence(topic, partition) {
      if (!isInitialized()) return SEQUENCE_START

      producerSequence[topic] = producerSequence[topic] || {}
      producerSequence[topic][partition] = producerSequence[topic][partition] || SEQUENCE_START

      return producerSequence[topic][partition]
    },

    updateSequence(topic, partition, sequence) {
      if (!isInitialized()) return

      producerSequence[topic] = producerSequence[topic] || {}
      producerSequence[topic][partition] = sequence
    },

    initProducerId: async () => {
      await cluster.refreshMetadata()
      // If non-transactional we can request the PID from any broker
      const broker = await cluster.findControllerBroker()
      const result = await broker.initProducerId({
        transactionTimeout: 30000,
      })

      producerId = result.producerId
      producerEpoch = result.producerEpoch
      producerSequence = {}

      logger.debug('Initialized producer id & epoch', { producerId, producerEpoch })
    },
  }
}
