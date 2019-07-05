module.exports = class SubscriptionState {
  constructor() {
    this.pausedPartitionsByTopic = {}
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  pause(topicPartitions = []) {
    topicPartitions.forEach(({ topic, partitions }) => {
      const pausedForTopic = this.pausedPartitionsByTopic[topic] || { topic, partitions: new Set() }
      partitions.forEach(partition => pausedForTopic.partitions.add(partition))
      this.pausedPartitionsByTopic[topic] = pausedForTopic
    })
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  resume(topicPartitions = []) {
    topicPartitions.forEach(({ topic, partitions }) => {
      const pausedForTopic = this.pausedPartitionsByTopic[topic] || { topic, partitions: new Set() }
      partitions.forEach(partition => pausedForTopic.partitions.delete(partition))
      this.pausedPartitionsByTopic[topic] = pausedForTopic
    })
  }

  /**
   * @returns {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  paused() {
    return Array.from(
      Object.values(this.topicPartitions).map(({ topic, partitions }) => {
        return {
          topic,
          partitions: Array.from(partitions.values()),
        }
      })
    )
  }

  isPaused(topic, partition) {
    let pausedTopicPartition = this.pausedPartitionsByTopic[topic]

    return !pausedTopicPartition || pausedTopicPartition.partitions.includes(partition)
  }
}
