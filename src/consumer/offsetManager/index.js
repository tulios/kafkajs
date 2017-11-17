const Long = require('long')
const initializeConsumerOffsets = require('./initializeConsumerOffsets')

const indexTopics = topics => topics.reduce((obj, topic) => assign(obj, { [topic]: {} }), {})
const { keys, assign } = Object

module.exports = class OffsetManager {
  constructor({
    cluster,
    coordinator,
    memberAssignment,
    topicConfigurations,
    groupId,
    generationId,
    memberId,
  }) {
    this.cluster = cluster
    this.coordinator = coordinator
    this.memberAssignment = memberAssignment
    this.topicConfigurations = topicConfigurations
    this.groupId = groupId
    this.generationId = generationId
    this.memberId = memberId

    this.topics = keys(memberAssignment)
    this.clearOffsets()
  }

  clearOffsets() {
    this.committedOffsets = indexTopics(this.topics)
    this.resolvedOffsets = indexTopics(this.topics)
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {object} offsets by topic and partition
   *                    {
   *                      'topic-name': {
   *                        0: '-1',
   *                        1: '10'
   *                      }
   *                    }
   */
  nextOffset(topic, partition) {
    // TODO: Handle latest and earliest offset
    if (!this.resolvedOffsets[topic][partition]) {
      this.resolvedOffsets[topic][partition] = this.committedOffsets[topic][partition]
    }

    let offset = this.resolvedOffsets[topic][partition]
    if (Long.fromValue(offset).equals(-1)) {
      offset = '0'
    }

    const nextOffset = Long.fromValue(offset)
      .add(1)
      .toString()

    this.resolvedOffsets[topic][partition] = nextOffset
    return offset
  }

  /**
   * @param {string} topic
   * @param {number} partition
   */
  resetOffset({ topic, partition }) {
    this.resolvedOffsets[topic][partition] = this.committedOffsets[topic][partition]
  }

  resolveOffset({ topic, partition, offset }) {
    this.resolvedOffsets[topic][partition] = Long.fromValue(offset)
      .add(1)
      .toString()
  }

  async commitOffsets() {
    const { groupId, generationId, memberId } = this

    const offsets = topic => keys(this.resolvedOffsets[topic])
    const emptyPartitions = ({ partitions }) => partitions.length > 0
    const toPartitions = topic => partition => ({
      partition,
      offset: this.resolvedOffsets[topic][partition],
    })
    const changedOffsets = topic => ({ partition, offset }) => {
      return (
        offset !== this.committedOffsets[topic][partition] &&
        Long.fromValue(offset).greaterThanOrEqual(0)
      )
    }

    // Select and format updated partitions
    const topicsWithPartitionsToCommit = this.topics
      .map(topic => ({
        topic,
        partitions: offsets(topic)
          .map(toPartitions(topic))
          .filter(changedOffsets(topic)),
      }))
      .filter(emptyPartitions)

    if (topicsWithPartitionsToCommit.length === 0) {
      return
    }

    await this.coordinator.offsetCommit({
      groupId,
      memberId,
      groupGenerationId: generationId,
      topics: topicsWithPartitionsToCommit,
    })

    // Update local reference of committed offsets
    topicsWithPartitionsToCommit.forEach(({ topic, partitions }) => {
      const updatedOffsets = partitions.reduce(
        (obj, { partition, offset }) => assign(obj, { [partition]: offset }),
        {}
      )
      assign(this.committedOffsets[topic], updatedOffsets)
    })
  }

  async resolveOffsets() {
    const { groupId } = this
    const invalidOffset = topic => partition => !/\d+/.test(this.committedOffsets[topic][partition])
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

    const { responses: consumerOffsets } = await this.coordinator.offsetFetch({
      groupId,
      topics: pendingPartitions,
    })

    const unresolvedPartitions = consumerOffsets.map(({ topic, partitions }) =>
      assign(
        {
          topic,
          partitions: partitions
            .filter(({ offset }) => Long.fromValue(offset).equals(-1))
            .map(({ partition }) => assign({ partition })),
        },
        this.topicConfigurations[topic]
      )
    )

    const indexPartitions = (obj, { partition, offset }) => {
      return assign(obj, { [partition]: offset })
    }

    let offsets = consumerOffsets
    if (unresolvedPartitions.length > 0) {
      const topicOffsets = await this.cluster.fetchTopicsOffset(unresolvedPartitions)
      offsets = initializeConsumerOffsets(consumerOffsets, topicOffsets)
    }

    offsets.forEach(({ topic, partitions }) => {
      this.committedOffsets[topic] = partitions.reduce(indexPartitions, {})
    })
  }
}
