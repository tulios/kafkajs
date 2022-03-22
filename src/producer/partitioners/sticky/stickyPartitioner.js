const StickyPartitionCache = require('./stickyPartitionCache')
const toPositive = require('../default/toPositive')
/**
 * Tries to batch messages to a single partition as an optimization
 */
module.exports = murmur2 => () => {
  const stickyPartitionCache = new StickyPartitionCache()

  // Used to try to detect batches of messages. Switches sticky partition when expires
  let lastPartitionTimeoutId = null
  let changeStickyPartition = true
  const EXPIRE_AFTER_MS = 500

  /**
   * @param opts {{
   *  topic: string;
   *  partitionMetadata: import("../../../../types").PartitionMetadata[];
   *  message: import("../../../../types").Message;
   * }}
   */
  return opts => {
    const { topic, partitionMetadata, message } = opts
    const numPartitions = partitionMetadata.length
    if (typeof message.partition === 'number') {
      return message.partition
    }

    if (typeof message.key === 'undefined' || message.key === null) {
      let result = null
      if (changeStickyPartition) {
        result = stickyPartitionCache.nextPartition(
          topic,
          partitionMetadata,
          stickyPartitionCache.indexCache[topic]
        )
        changeStickyPartition = false
      } else {
        result = stickyPartitionCache.partition(topic, partitionMetadata)
      }

      // If no requests follow this one, use a new sticky partition
      clearTimeout(lastPartitionTimeoutId)
      lastPartitionTimeoutId = setTimeout(() => {
        changeStickyPartition = true
      }, EXPIRE_AFTER_MS)

      return result
    }

    return toPositive(murmur2(message.key)) % numPartitions
  }
}
