const randomBytes = require('../default/randomBytes')
const toPositive = require('../default/toPositive')
/**
 * Based on https://github.com/a0x8o/kafka/blob/master/clients/src/main/java/org/apache/kafka/clients/producer/internals/StickyPartitionCache.java
 */
module.exports = class StickyPartitionCache {
  constructor() {
    /** @type {Record<string, number>} */
    this.indexCache = {}
  }

  /**
   * @param topic {string}
   * @param partitionMetadata {import("../../../../types").PartitionMetadata[]} partitions for topic
   * @returns {number}
   */
  partition(topic, partitionMetadata) {
    const partition = this.indexCache[topic]
    if (typeof partition === 'undefined') {
      return this.nextPartition(topic, partitionMetadata, -1)
    }
    return partition
  }

  /**
   * @param topic {string}
   * @param partitionMetadata {import("../../../../types").PartitionMetadata[]} partitions for topic
   * @param prevPartition {number}
   * @returns {number}
   */
  nextPartition(topic, partitionMetadata, prevPartition) {
    const oldPartition = this.indexCache[topic]
    let newPartition = oldPartition

    /* Check that the current sticky partition for the topic is either not set or that the partition
     * that triggered the new batch matches the sticky partition that needs to be changed. */
    if (typeof oldPartition === 'undefined' || oldPartition === prevPartition) {
      const availablePartitions = partitionMetadata.filter(p => p.leader >= 0)
      if (!availablePartitions.length) {
        const random = toPositive(randomBytes(32).readUInt32BE(0))
        newPartition = random % partitionMetadata.length
      } else if (availablePartitions.length === 1) {
        newPartition = availablePartitions[0].partitionId
      } else {
        while (typeof newPartition === 'undefined' || newPartition === oldPartition) {
          const random = toPositive(randomBytes(32).readUInt32BE(0))
          newPartition = availablePartitions[random % availablePartitions.length].partitionId
        }
      }

      // Only change the sticky partition if it is null or previous matches the current sticky partition
      if (typeof oldPartition === 'undefined') {
        if (typeof this.indexCache[topic] === 'undefined') {
          this.indexCache[topic] = newPartition
        }
      } else if (this.indexCache[topic] === prevPartition) {
        this.indexCache[topic] = newPartition
      }
    }

    return this.indexCache[topic]
  }
}
