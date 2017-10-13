const Connection = require('./network/connection')
const Broker = require('./broker')

const createConnection = (cluster, { host, port, rack }) =>
  new Connection({
    host,
    port,
    rack,
    ssl: cluster.ssl,
    sasl: cluster.sasl,
    logger: cluster.logger,
  })

module.exports = class Cluster {
  constructor({ host, port, ssl, sasl, logger }) {
    this.host = host
    this.port = port
    this.ssl = ssl
    this.sasl = sasl
    this.logger = logger

    this.seedBroker = new Broker(createConnection(this, { host, port }))
    this.targetTopics = new Set()
    this.brokerPool = {}

    this.versions = null
    this.metadata = null
  }

  async connect() {
    await this.seedBroker.connect()
    this.versions = this.seedBroker.versions
  }

  async disconnect() {
    await this.seedBroker.disconnect()
    await Promise.all(Object.values(this.brokerPool).map(broker => broker.disconnect()))
    this.brokerPool = {}
    this.metadata = {}
  }

  async refreshMetadata() {
    this.metadata = await this.seedBroker.metadata(Array.from(this.targetTopics))
    this.brokerPool = this.metadata.brokers.reduce((result, { nodeId, host, port, rack }) => {
      if (result[nodeId]) {
        return result
      }

      const connection = createConnection(this, { host, port, rack })
      return Object.assign(result, {
        [nodeId]: new Broker(connection, this.versions),
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
