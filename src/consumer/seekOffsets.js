const { entries, keys } = Object

module.exports = class SeekOffsets {
  constructor() {
    this.data = {}
  }

  getTopicPartitions() {
    return entries(this.data).map(([topic, partitions]) => ({
      topic,
      partitions: keys(partitions),
    }))
  }

  set(topic, partition, offset) {
    if (!(topic in this.data)) this.data[topic] = {}
    this.data[topic][partition] = offset
  }

  has(topic, partition) {
    return topic in this.data && partition in this.data[topic]
  }

  pop(topic, partition) {
    if (!(topic in this.data)) return
    if (!(partition in this.data[topic])) return

    const offset = this.data[topic][partition]
    delete this.data[topic][partition]

    return { topic, partition, offset }
  }
}
