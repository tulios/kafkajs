const Long = require('../../utils/long')
const isInvalidOffset = require('./isInvalidOffset')
const initializeConsumerOffsets = require('./initializeConsumerOffsets')

const { keys, assign } = Object
const indexTopics = topics => topics.reduce((obj, topic) => assign(obj, { [topic]: {} }), {})

module.exports = class ManualOffsetManager {
  /**
   * @param {Object} options
   * @param {import("../../../types").Cluster} options.cluster
   * @param {import("../../../types").IMemberAssignment} options.memberAssignment
   * @param {{[topic: string]: { fromBeginning: boolean }}} options.topicConfigurations
   * @param {import("../../instrumentation/emitter")} options.instrumentationEmitter
   * @param {number} options.generationId
   */
  constructor({ cluster, memberAssignment, topicConfigurations, instrumentationEmitter }) {
    this.cluster = cluster

    // memberAssignment format:
    // {
    //   'topic1': [0, 1, 2, 3],
    //   'topic2': [0, 1, 2, 3, 4, 5],
    // }
    this.memberAssignment = memberAssignment

    this.topicConfigurations = topicConfigurations
    this.instrumentationEmitter = instrumentationEmitter
    this.topics = keys(memberAssignment)
    this.resolvedOffsets = indexTopics(this.topics)
  }

  updateMemberAssignment(memberAssignment) {
    this.memberAssignment = memberAssignment
    this.topics = keys(memberAssignment)
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {Long}
   */
  nextOffset(topic, partition) {
    let offset = this.resolvedOffsets[topic][partition]
    if (isInvalidOffset(offset)) {
      offset = '0'
    }

    return Long.fromValue(offset)
  }

  /**
   * @param {import("../../../types").TopicPartitionOffset} topicPartitionOffset
   */
  resolveOffset({ topic, partition, offset }) {
    this.resolvedOffsets[topic][partition] = Long.fromValue(offset)
      .add(1)
      .toString()
  }

  /**
   * @param {import("../../../types").TopicPartition} topicPartition
   */
  setDefaultOffset({ topic, partition }) {
    const defaultOffset = this.cluster.defaultOffset(this.topicConfigurations[topic])
    this.resolvedOffsets[topic][partition] = Long.fromValue(defaultOffset).toString()
  }

  /**
   * Set the offset to be used for the next fetch. If the consumer isn't assigned to the given
   * topic/partition this method will be a NO-OP.
   *
   * @param {import("../../../types").TopicPartitionOffset} topicPartitionOffset
   */
  async seek({ topic, partition, offset }) {
    if (!this.memberAssignment[topic] || !this.memberAssignment[topic].includes(partition)) {
      return
    }

    this.resolveOffset({
      topic,
      partition,
      offset: Long.fromValue(offset)
        .subtract(1)
        .toString(),
    })
  }

  /**
   * resolveOffsets can be called when first initializing the consumer, in which case all partitions are pending,
   * or when partitions have been added or removed. Newly added partitions are pending.
   */
  async resolveOffsets() {
    const invalidOffset = topic => partition => {
      return isInvalidOffset(this.resolvedOffsets[topic][partition])
    }

    const pendingPartitions = this.topics
      .map(topic => ({
        topic,
        partitions: this.memberAssignment[topic]
          .filter(invalidOffset(topic))
          .map(partition => ({ partition })),
      }))
      .filter(t => t.partitions.length > 0)

    if (pendingPartitions.length === 0) {
      return
    }

    const unresolvedPartitions = pendingPartitions.map(({ topic, partitions }) =>
      assign(
        {
          topic,
          partitions,
        },
        this.topicConfigurations[topic]
      )
    )

    const indexPartitions = (obj, { partition, offset }) => {
      return assign(obj, { [partition]: offset })
    }

    const hasUnresolvedPartitions = () => unresolvedPartitions.some(t => t.partitions.length > 0)

    if (hasUnresolvedPartitions()) {
      const topicOffsets = await this.cluster.fetchTopicsOffset(unresolvedPartitions)
      const offsets = initializeConsumerOffsets(topicOffsets, topicOffsets)
      offsets.forEach(({ topic, partitions }) => {
        this.resolvedOffsets[topic] = partitions.reduce(indexPartitions, {
          ...this.resolvedOffsets[topic],
        })
      })
    }
  }

  /**
   * @private
   * @param {import("../../../types").TopicPartition} topicPartition
   */
  clearOffsets({ topic, partition }) {
    delete this.resolvedOffsets[topic][partition]
  }
}
