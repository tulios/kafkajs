module.exports = ({ logger, cluster }) => {
  let producerId = -1
  let producerEpoch = 0
  let sequences = {}

  return {
    getProducerId() {
      return producerId
    },

    getProducerEpoch() {
      return producerEpoch
    },

    getSequence(topic, partition) {
      sequences[topic] = sequences[topic] || {}
      sequences[topic][partition] = sequences[topic][partition] || 0

      return sequences[topic][partition]
    },

    updateSequence(topic, partition, sequence) {
      sequences[topic] = sequences[topic] || {}
      sequences[topic][partition] = sequence
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
      sequences = {}

      logger.debug('Initialized producer id & epoch', producerId, producerEpoch)
    },
  }
}
