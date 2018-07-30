const Long = require('long')

module.exports = class Batch {
  constructor(topic, fetchedOffset, partitionData) {
    const longFetchedOffset = Long.fromValue(fetchedOffset)

    this.topic = topic
    this.partition = partitionData.partition
    this.highWatermark = partitionData.highWatermark

    // Apparently fetch can return different offsets than the target offset provided to the fetch API.
    // Discard messages that are not in the requested offset
    // https://github.com/apache/kafka/blob/bf237fa7c576bd141d78fdea9f17f65ea269c290/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L912
    this.messages = partitionData.messages.filter(message =>
      Long.fromValue(message.offset).gte(longFetchedOffset)
    )
  }

  isEmpty() {
    return this.messages.length === 0
  }

  firstOffset() {
    return this.isEmpty() ? null : this.messages[0].offset
  }

  lastOffset() {
    return this.isEmpty()
      ? Long.fromValue(this.highWatermark)
          .add(-1)
          .toString()
      : this.messages[this.messages.length - 1].offset
  }

  offsetLag() {
    if (this.isEmpty()) {
      return '0'
    }

    const lastOffsetOfPartition = Long.fromValue(this.highWatermark).add(-1)
    const lastConsumedOffset = Long.fromValue(this.lastOffset())
    return lastOffsetOfPartition.add(lastConsumedOffset.multiply(-1)).toString()
  }
}
