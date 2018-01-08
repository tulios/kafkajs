module.exports = class SeekOffsets extends Map {
  set(topic, partition, offset) {
    super.set([topic, partition], offset)
  }

  pop() {
    if (this.size === 0) {
      return
    }

    const [key, offset] = Array.from(this.entries())
      .reverse()
      .pop()
    this.delete(key)
    const [topic, partition] = key
    return { topic, partition, offset }
  }
}
