const Long = require('long')
const flatten = require('../../utils/flatten')
const isInvalidOffset = require('./isInvalidOffset')
const initializeConsumerOffsets = require('./initializeConsumerOffsets')
const Lock = require('../../utils/lock')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')

const { keys, assign } = Object
const indexTopics = topics => topics.reduce((obj, topic) => assign(obj, { [topic]: {} }), {})

const PRIVATE = {
  COMMITTED_OFFSETS: Symbol('private:OffsetManager:committedOffsets'),
}
module.exports = class OffsetManager {
  constructor({
    cluster,
    coordinator,
    memberAssignment,
    autoCommitInterval,
    autoCommitThreshold,
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

    this.autoCommitInterval = autoCommitInterval
    this.autoCommitThreshold = autoCommitThreshold
    this.lastCommit = Date.now()

    this.topics = keys(memberAssignment)
    this.clearAllOffsets()
    this.lock = new Lock({
      description: `offsetManager-${groupId}-${generationId}-${memberId}`,
    })
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {Long}
   */
  nextOffset(topic, partition) {
    if (!this.resolvedOffsets[topic][partition]) {
      this.resolvedOffsets[topic][partition] = this.committedOffsets()[topic][partition]
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
    this.resolvedOffsets[topic][partition] = this.committedOffsets()[topic][partition]
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
   * @returns {Long}
   */
  countResolvedOffsets() {
    const toPartitions = topic => keys(this.resolvedOffsets[topic])

    const subtractOffsets = (resolvedOffset, committedOffset) => {
      const resolvedOffsetLong = Long.fromValue(resolvedOffset)
      return committedOffset === '-1'
        ? resolvedOffsetLong
        : resolvedOffsetLong.subtract(Long.fromValue(committedOffset))
    }

    const subtractPartitionOffsets = (topic, partition) =>
      subtractOffsets(
        this.resolvedOffsets[topic][partition],
        this.committedOffsets()[topic][partition]
      )

    const subtractTopicOffsets = topic =>
      toPartitions(topic).map(partition => subtractPartitionOffsets(topic, partition))

    const offsetsDiff = this.topics.map(subtractTopicOffsets)
    return flatten(offsetsDiff).reduce((sum, offset) => sum.add(offset), Long.fromValue(0))
  }

  /**
   * @param {string} topic
   * @param {number} partition
   */
  async setDefaultOffset({ topic, partition }) {
    try {
      await this.lock.acquire()
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
    } finally {
      await this.lock.release()
    }
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
    try {
      await this.lock.acquire()
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
    } finally {
      await this.lock.release()
    }
  }

  async commitOffsetsIfNecessary() {
    const now = Date.now()

    const timeoutReached =
      this.autoCommitInterval != null && now >= this.lastCommit + this.autoCommitInterval

    const thresholdReached =
      this.autoCommitThreshold != null &&
      this.countResolvedOffsets().gte(Long.fromValue(this.autoCommitThreshold))

    if (timeoutReached || thresholdReached) {
      return this.commitOffsets()
    }
  }

  /**
   * Return all locally resolved offsets which are not marked as committed, by topic-partition.
   * @returns {OffsetsByTopicPartition}
   *
   * @typedef {Object} OffsetsByTopicPartition
   * @property {TopicOffsets[]} topics
   *
   * @typedef {Object} TopicOffsets
   * @property {PartitionOffset[]} partitions
   *
   * @typedef {Object} PartitionOffset
   * @property {string} partition
   * @property {string} offset
   */
  uncommittedOffsets() {
    const offsets = topic => keys(this.resolvedOffsets[topic])
    const emptyPartitions = ({ partitions }) => partitions.length > 0
    const toPartitions = topic => partition => ({
      partition,
      offset: this.resolvedOffsets[topic][partition],
    })
    const changedOffsets = topic => ({ partition, offset }) => {
      return (
        offset !== this.committedOffsets()[topic][partition] &&
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

    return { topics: topicsWithPartitionsToCommit }
  }

  async commitOffsets(offsets = {}) {
    const { groupId, generationId, memberId } = this
    const { topics = this.uncommittedOffsets().topics } = offsets

    if (topics.length === 0) {
      this.lastCommit = Date.now()
      return
    }

    const payload = {
      groupId,
      memberId,
      groupGenerationId: generationId,
      topics,
    }

    try {
      const coordinator = await this.getCoordinator()
      await coordinator.offsetCommit(payload)
      this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload)

      // Update local reference of committed offsets
      topics.forEach(({ topic, partitions }) => {
        const updatedOffsets = partitions.reduce(
          (obj, { partition, offset }) => assign(obj, { [partition]: offset }),
          {}
        )
        assign(this.committedOffsets()[topic], updatedOffsets)
      })

      this.lastCommit = Date.now()
    } catch (e) {
      // metadata is stale, the coordinator has changed due to a restart or
      // broker reassignment
      if (e.type === 'NOT_COORDINATOR_FOR_GROUP') {
        await this.cluster.refreshMetadata()
      }

      throw e
    }
  }

  async resolveOffsets() {
    try {
      await this.lock.acquire()

      const { groupId } = this
      const invalidOffset = topic => partition => {
        return isInvalidOffset(this.committedOffsets()[topic][partition])
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
        this.committedOffsets()[topic] = partitions.reduce(indexPartitions, {
          ...this.committedOffsets()[topic],
        })
      })
    } finally {
      await this.lock.release()
    }
  }

  /**
   * @private
   * @param {string} topic
   * @param {number} partition
   */
  clearOffsets({ topic, partition }) {
    delete this.committedOffsets()[topic][partition]
    delete this.resolvedOffsets[topic][partition]
  }

  /**
   * @private
   */
  clearAllOffsets() {
    const committedOffsets = this.committedOffsets()

    for (const topic in committedOffsets) {
      delete committedOffsets[topic]
    }

    for (const topic of this.topics) {
      committedOffsets[topic] = {}
    }

    this.resolvedOffsets = indexTopics(this.topics)
  }

  committedOffsets() {
    if (!this[PRIVATE.COMMITTED_OFFSETS]) {
      this[PRIVATE.COMMITTED_OFFSETS] = this.groupId
        ? this.cluster.committedOffsets({ groupId: this.groupId })
        : {}
    }

    return this[PRIVATE.COMMITTED_OFFSETS]
  }
}
