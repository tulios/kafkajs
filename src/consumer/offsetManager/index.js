const Long = require('long')
const isInvalidOffset = require('./isInvalidOffset')
const initializeConsumerOffsets = require('./initializeConsumerOffsets')
const { COMMIT_OFFSETS } = require('../instrumentationEvents')

const { keys, assign } = Object
const indexTopics = topics => topics.reduce((obj, topic) => assign(obj, { [topic]: {} }), {})

module.exports = class OffsetManager {
  constructor({
    cluster,
    coordinator,
    memberAssignment,
    topicConfigurations,
    instrumentationEmitter,
    groupId,
    generationId,
    memberId,
  }) {
    this.cluster = cluster
    this.coordinator = coordinator

    // memberAssignment format:
    // {
    //   'topic1': [0, 1, 2, 3],
    //   'topic2': [0, 1, 2, 3, 4, 5],
    // }
    this.memberAssignment = memberAssignment

    this.topicConfigurations = topicConfigurations
    this.instrumentationEmitter = instrumentationEmitter
    this.groupId = groupId
    this.generationId = generationId
    this.memberId = memberId

    this.topics = keys(memberAssignment)
    this.clearAllOffsets()
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {string}
   */
  nextOffset(topic, partition) {
    if (!this.resolvedOffsets[topic][partition]) {
      this.resolvedOffsets[topic][partition] = this.committedOffsets[topic][partition]
    }

    let offset = this.resolvedOffsets[topic][partition]
    if (Long.fromValue(offset).equals(-1)) {
      offset = '0'
    }

    return Long.fromValue(offset)
  }

  /**
   * @returns {Broker}
   */
  async getCoordinator() {
    if (!this.coordinator.isConnected()) {
      this.coordinator = await this.cluster.findBroker(this.coordinator)
    }

    return this.coordinator
  }

  /**
   * @param {string} topic
   * @param {number} partition
   */
  resetOffset({ topic, partition }) {
    this.resolvedOffsets[topic][partition] = this.committedOffsets[topic][partition]
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @param {string} offset
   */
  resolveOffset({ topic, partition, offset }) {
    this.resolvedOffsets[topic][partition] = Long.fromValue(offset)
      .add(1)
      .toString()
  }

  /**
   * @param {string} topic
   * @param {number} partition
   */
  async setDefaultOffset({ topic, partition }) {
    const { groupId, generationId, memberId } = this
    const defaultOffset = this.cluster.defaultOffset(this.topicConfigurations[topic])
    const coordinator = await this.getCoordinator()

    await coordinator.offsetCommit({
      groupId,
      memberId,
      groupGenerationId: generationId,
      topics: [
        {
          topic,
          partitions: [{ partition, offset: defaultOffset }],
        },
      ],
    })

    this.clearOffsets({ topic, partition })
  }

  /**
   * Commit the given offset to the topic/partition. If the consumer isn't assigned to the given
   * topic/partition this method will be a NO-OP.
   *
   * @param {string} topic
   * @param {number} partition
   * @param {string} offset
   */
  async seek({ topic, partition, offset }) {
    if (!this.memberAssignment[topic] || !this.memberAssignment[topic].includes(partition)) {
      return
    }

    const { groupId, generationId, memberId } = this
    const coordinator = await this.getCoordinator()

    await coordinator.offsetCommit({
      groupId,
      memberId,
      groupGenerationId: generationId,
      topics: [
        {
          topic,
          partitions: [{ partition, offset }],
        },
      ],
    })

    this.clearOffsets({ topic, partition })
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

    const payload = {
      groupId,
      memberId,
      groupGenerationId: generationId,
      topics: topicsWithPartitionsToCommit,
    }

    const coordinator = await this.getCoordinator()
    await coordinator.offsetCommit(payload)
    this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload)

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
    const invalidOffset = topic => partition => {
      return isInvalidOffset(this.committedOffsets[topic][partition])
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

    const coordinator = await this.getCoordinator()
    const { responses: consumerOffsets } = await coordinator.offsetFetch({
      groupId,
      topics: pendingPartitions,
    })

    const unresolvedPartitions = consumerOffsets.map(({ topic, partitions }) =>
      assign(
        {
          topic,
          partitions: partitions
            .filter(({ offset }) => isInvalidOffset(offset))
            .map(({ partition }) => assign({ partition })),
        },
        this.topicConfigurations[topic]
      )
    )

    const indexPartitions = (obj, { partition, offset }) => {
      return assign(obj, { [partition]: offset })
    }

    const hasUnresolvedPartitions = () =>
      unresolvedPartitions.filter(t => t.partitions.length > 0).length > 0

    let offsets = consumerOffsets
    if (hasUnresolvedPartitions()) {
      const topicOffsets = await this.cluster.fetchTopicsOffset(unresolvedPartitions)
      offsets = initializeConsumerOffsets(consumerOffsets, topicOffsets)
    }

    offsets.forEach(({ topic, partitions }) => {
      this.committedOffsets[topic] = partitions.reduce(indexPartitions, {})
    })
  }

  /**
   * @private
   * @param {string} topic
   * @param {number} partition
   */
  clearOffsets({ topic, partition }) {
    delete this.committedOffsets[topic][partition]
    delete this.resolvedOffsets[topic][partition]
  }

  /**
   * @private
   */
  clearAllOffsets() {
    this.committedOffsets = indexTopics(this.topics)
    this.resolvedOffsets = indexTopics(this.topics)
  }
}
