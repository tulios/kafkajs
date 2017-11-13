const Long = require('long')

module.exports = class Batch {
  constructor(topic, partitionData) {
    this.topic = topic
    this.partition = partitionData.partition
    this.highWatermark = partitionData.highWatermark
    this.messages = partitionData.messages
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
}
