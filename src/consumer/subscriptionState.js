const { KafkaJSNonRetriableError } = require('../errors')

module.exports = class SubscriptionState {
  constructor() {
    this.assignedPartitionsByTopic = {}
    this.pausedPartitionsByTopic = {}
  }

  /**
   * Replace the current assignment with a new set of assignments
   *
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  assign(topicPartitions = []) {
    this.assignedPartitionsByTopic = topicPartitions.reduce((assigned, { topic, partitions }) => {
      return { ...assigned, [topic]: { topic, partitions } }
    }, {})
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  pause(topicPartitions = []) {
    topicPartitions.forEach(({ topic, partitions }) => {
      const pausedForTopic = this.pausedPartitionsByTopic[topic] || {
        topic,
        partitions: new Set(),
        all: false,
      }

      if (typeof partitions === 'undefined') {
        pausedForTopic.partitions.clear()
        pausedForTopic.all = true
      } else if (Array.isArray(partitions)) {
        partitions.forEach(partition => pausedForTopic.partitions.add(partition))
        pausedForTopic.all = false
      }

      this.pausedPartitionsByTopic[topic] = pausedForTopic
    })
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  resume(topicPartitions = []) {
    topicPartitions.forEach(({ topic, partitions }) => {
      const pausedForTopic = this.pausedPartitionsByTopic[topic] || { topic, partitions: new Set() }

      if (typeof partitions === 'undefined') {
        pausedForTopic.partitions.clear()
        pausedForTopic.all = false
      } else if (Array.isArray(partitions) && !pausedForTopic.all) {
        partitions.forEach(partition => pausedForTopic.partitions.delete(partition))
      } else if (Array.isArray(partitions) && pausedForTopic.all) {
        // TODO: Remove this after we've moved member-assignment state into here
        // (https://github.com/tulios/kafkajs/issues/427) as that should allow us to correctly resume
        // partitions after pausing an entire topic (by tracking resumed partitions as well as paused).
        throw new KafkaJSNonRetriableError(
          'Can not resume specific partitions of topic when entire topic was paused before'
        )
      }

      this.pausedPartitionsByTopic[topic] = pausedForTopic
    })
  }

  /**
   * @returns {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  assigned() {
    return Object.values(this.assignedPartitionsByTopic).map(({ topic, partitions }) => {
      return {
        topic,
        partitions: Array.from(partitions.values()),
      }
    })
  }

  /**
   * @returns {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  active() {
    return Object.values(this.assignedPartitionsByTopic).map(({ topic, partitions }) => {
      return {
        topic,
        partitions: partitions.filter(partition => !this.isPaused(topic, partition)),
      }
    })
  }

  /**
   * @returns {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  paused() {
    return Object.values(this.assignedPartitionsByTopic).map(({ topic, partitions }) => {
      return {
        topic,
        partitions: partitions.filter(partition => this.isPaused(topic, partition)),
      }
    })
  }

  isPaused(topic, partition) {
    let paused = this.pausedPartitionsByTopic[topic]

    return !!(paused && (paused.all || paused.partitions.has(partition)))
  }
}
