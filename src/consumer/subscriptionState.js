const { KafkaJSNonRetriableError } = require('../errors')

module.exports = class SubscriptionState {
  constructor() {
    this.pausedPartitionsByTopic = {}
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
      } else if (Array.isAray(partitions) && !pausedForTopic.all) {
        partitions.forEach(partition => pausedForTopic.partitions.delete(partition))
      } else if (Array.isArray(partitions) && pausedForTopic.all) {
        // TODO: consider whether we should actively track active topics, rather than paused ones, as to avoid this,
        // or perhaps a "whitelist" and "blacklist" of either, to allow for pausing toppars we haven't had assigned yet
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
  paused() {
    return Object.values(this.pausedPartitionsByTopic).map(({ topic, partitions }) => {
      return {
        topic,
        partitions: Array.from(partitions.values()),
      }
    })
  }

  isPaused(topic, partition) {
    let paused = this.pausedPartitionsByTopic[topic]

    return !!(paused && (paused.all || paused.partitions.has(partition)))
  }
}
