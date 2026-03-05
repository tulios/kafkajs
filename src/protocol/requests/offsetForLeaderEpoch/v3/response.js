const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, errorCodes } = require('../../../error')

/**
 * OffsetsForLeaderEpoch Response (Version: 3) => [topics] TAG_BUFFER
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => error_code partition leader_epoch end_offset TAG_BUFFER
 *       error_code => INT16
 *       partition => INT32
 *       leader_epoch => INT32
 *       end_offset => INT64
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const result = {
    topics: decoder.readUVarIntArray(decodeTopics),
  }
  decoder.readTaggedFields()
  return result
}

const decodeTopics = decoder => {
  const topic = {
    topic: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions),
  }
  decoder.readTaggedFields()
  return topic
}

const decodePartitions = decoder => {
  const partition = {
    errorCode: decoder.readInt16(),
    partition: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
    endOffset: decoder.readInt64(),
  }
  decoder.readTaggedFields()
  return partition
}

const parse = async data => {
  const topicsWithErrors = data.topics.flatMap(({ topic, partitions }) =>
    partitions
      .filter(({ errorCode }) => failure(errorCode))
      .map(partition => ({
        ...partition,
        topic,
      }))
  )

  if (topicsWithErrors.length > 0) {
    const errors = topicsWithErrors.map(
      ({ topic, partition, errorCode }) =>
        `${topic}[${partition}]: ${createErrorFromCode(errorCode).message}`
    )
    throw createErrorFromCode(topicsWithErrors[0].errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
