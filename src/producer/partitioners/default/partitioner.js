const randomBytes = require('./randomBytes')
const toPositive = require('./toPositive')

// Based on the java client 0.10.2
// https://github.com/apache/kafka/blob/0.10.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java

/**
 * The default partitioning strategy:
 *  - If a partition is specified in the message, use it
 *  - If no partition is specified but a key is present choose a partition based on a hash of the key
 *  - If no partition or key is present choose a partition in a round-robin fashion
 */
module.exports = murmur2 => () => {
  const counters = {}

  return ({ topic, partitionMetadata, message }) => {
    if (!(topic in counters)) {
      counters[topic] = randomBytes(32).readUInt32BE(0)
    }
    const numPartitions = partitionMetadata.length
    const availablePartitions = partitionMetadata.filter(p => p.leader >= 0)
    const numAvailablePartitions = availablePartitions.length

    if (message.partition !== null && message.partition !== undefined) {
      return message.partition
    }

    if (message.key !== null && message.key !== undefined) {
      return toPositive(murmur2(message.key)) % numPartitions
    }

    if (numAvailablePartitions > 0) {
      const i = toPositive(++counters[topic]) % numAvailablePartitions
      return availablePartitions[i].partitionId
    }

    // no partitions are available, give a non-available partition
    return toPositive(++counters[topic]) % numPartitions
  }
}
