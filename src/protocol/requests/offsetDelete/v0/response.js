const { KafkaJSAggregateError, KafkaJSOffsetDeleteError } = require('../../../../errors')
const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * OffsetDelete Response (Version: 0) => error_code throttle_time_ms [topics]
 *   error_code => INT16
 *   throttle_time_ms => INT32
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index error_code
 *       partition_index => INT32
 *       error_code => INT16
 */

/**
 * @param {Decoder} decoder
 */
const decodePartition = decoder => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
  }

  return partition
}

/**
 * @param {Decoder} decoder
 */
const decodeTopics = decoder => {
  const topic = {
    topic: decoder.readString(),
    partitions: decoder.readArray(decodePartition),
  }

  return topic
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)

  const errorCode = decoder.readInt16()
  const throttleTime = decoder.readInt32()
  return {
    errorCode,
    throttleTime,
    topics: decoder.readArray(decodeTopics),
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  const topicPartitionsWithError = data.topics.flatMap(({ topic, partitions }) =>
    partitions
      .filter(partition => failure(partition.errorCode))
      .map(partition => ({
        ...partition,
        topic,
      }))
  )

  if (topicPartitionsWithError.length > 0) {
    throw new KafkaJSAggregateError(
      'Errors deleting offsets',
      topicPartitionsWithError.map(
        ({ topic, partition, errorCode }) =>
          new KafkaJSOffsetDeleteError(createErrorFromCode(errorCode), topic, partition)
      )
    )
  }

  return data
}

module.exports = {
  decode,
  parse,
}
