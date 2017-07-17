const Connection = require('./connection')
const Broker = require('./broker')
const loadApiVersions = require('./broker/apiVersions')

module.exports = class Cluster {
  constructor({ host, port, logger }) {
    this.host = host
    this.port = port
    this.logger = logger

    this.seedConnection = new Connection({ host, port, logger: this.logger })
    this.targetTopics = new Set()
    this.seedBroker = null
    this.versions = null
    this.metadata = null
    this.brokerPool = {}
  }

  async connect() {
    await this.seedConnection.connect()
    this.versions = await loadApiVersions(this.seedConnection)
    this.seedBroker = new Broker(this.seedConnection, this.versions)
  }

  async disconnect() {
    this.seedConnection.disconnect()
    Object.values(this.brokerPool).forEach(broker => broker.disconnect())
    this.brokerPool = {}
    this.metadata = {}
  }

  async refreshMetadata() {
    this.metadata = await this.seedBroker.metadata(Array.from(this.targetTopics))
    this.brokerPool = this.metadata.brokers.reduce((result, { nodeId, host, port, rack }) => {
      if (result[nodeId]) {
        return result
      }

      return Object.assign(result, {
        [nodeId]: new Broker(
          new Connection({ host, port, rack, logger: this.logger }),
          this.versions
        ),
      })
    }, this.brokerPool)
  }

  async addTargetTopic(topic) {
    const previousSize = this.targetTopics.size
    this.targetTopics.add(topic)
    const hasChanged = previousSize !== this.targetTopics.size

    if (hasChanged) {
      await this.refreshMetadata()
    }
  }

  async findBroker({ nodeId }) {
    const broker = this.brokerPool[nodeId]
    await broker.connect()
    return broker
  }

  findTopicPartitionMetadata(topic) {
    return this.metadata.topicMetadata.find(t => t.topic === topic).partitionMetadata
  }

  findLeaderForPartitions(topic, partitions) {
    const partitionMetadata = this.findTopicPartitionMetadata(topic)
    return partitions.reduce((result, id) => {
      const partitionId = parseInt(id, 10)
      const { leader } = partitionMetadata.find(p => p.partitionId === partitionId)
      const current = result[leader] || []
      return Object.assign(result, { [leader]: [...current, partitionId] })
    }, {})
  }
}
