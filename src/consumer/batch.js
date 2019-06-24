const Long = require('long')
const filterAbortedMessages = require('./filterAbortedMessages')

/**
 * A batch collects messages returned from a single fetch call.
 *
 * A batch could contain _multiple_ Kafka RecordBatches.
 */
module.exports = class Batch {
  constructor(topic, fetchedOffset, partitionData) {
    const longFetchedOffset = Long.fromValue(fetchedOffset)
    const { abortedTransactions } = partitionData

    this.topic = topic
    this.partition = partitionData.partition
    this.highWatermark = partitionData.highWatermark

    // Apparently fetch can return different offsets than the target offset provided to the fetch API.
    // Discard messages that are not in the requested offset
    // https://github.com/apache/kafka/blob/bf237fa7c576bd141d78fdea9f17f65ea269c290/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L912
    const messagesWithinOffset = partitionData.messages.filter(message =>
      Long.fromValue(message.offset).gte(longFetchedOffset)
    )
    this.unfilteredMessages = messagesWithinOffset

    // 1. Don't expose aborted messages
    // 2. Don't expose control records
    // @see https://kafka.apache.org/documentation/#controlbatch
    this.messages = filterAbortedMessages({
      messages: messagesWithinOffset,
      abortedTransactions,
    }).filter(message => !message.isControlRecord)
  }

  isEmpty() {
    return this.messages.length === 0
  }

  isEmptyIncludingFiltered() {
    return this.unfilteredMessages.length === 0
  }

  firstOffset() {
    return this.isEmptyIncludingFiltered() ? null : this.unfilteredMessages[0].offset
  }

  lastOffset() {
    return this.isEmptyIncludingFiltered()
      ? Long.fromValue(this.highWatermark)
          .add(-1)
          .toString()
      : this.unfilteredMessages[this.unfilteredMessages.length - 1].offset
  }

  /**
   * Returns the lag based on the last offset in the batch (also known as "high")
   */
  offsetLag() {
    if (this.isEmptyIncludingFiltered()) {
      return '0'
    }

    const lastOffsetOfPartition = Long.fromValue(this.highWatermark).add(-1)
    const lastConsumedOffset = Long.fromValue(this.lastOffset())
    return lastOffsetOfPartition.add(lastConsumedOffset.multiply(-1)).toString()
  }

  /**
   * Returns the lag based on the first offset in the batch
   */
  offsetLagLow() {
    if (this.isEmptyIncludingFiltered()) {
      return '0'
    }

    const lastOffsetOfPartition = Long.fromValue(this.highWatermark).add(-1)
    const firstConsumedOffset = Long.fromValue(this.firstOffset())
    return lastOffsetOfPartition.add(firstConsumedOffset.multiply(-1)).toString()
  }
}
