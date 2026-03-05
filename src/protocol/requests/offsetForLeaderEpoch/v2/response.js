const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, errorCodes } = require('../../../error')

/**
 * OffsetsForLeaderEpoch Response (Version: 2) => [topics]
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => error_code partition leader_epoch end_offset
 *       error_code => INT16
 *       partition => INT32
 *       leader_epoch => INT32
 *       end_offset => INT64
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    topics: decoder.readArray(decodeTopics),
  }
}

const decodeTopics = decoder => ({
  topic: decoder.readString(),
  partitions: decoder.readArray(decodePartitions),
})

const decodePartitions = decoder => ({
  errorCode: decoder.readInt16(),
  partition: decoder.readInt32(),
  leaderEpoch: decoder.readInt32(),
  endOffset: decoder.readInt64(),
})

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
