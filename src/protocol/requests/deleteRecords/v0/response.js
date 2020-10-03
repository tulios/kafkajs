const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * DeleteRecords Response (Version: 0) => throttle_time_ms [topics]
 *  throttle_time_ms => INT32
 *  topics => name [partitions]
 *    name => STRING
 *    partitions => partition low_watermark error_code
 *      partition => INT32
 *      low_watermark => INT64
 *      error_code => INT16
 */

const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    throttleTime: decoder.readInt32(),
    topics: decoder
      .readArray(decoder => ({
        topic: decoder.readString(),
        partitions: decoder.readArray(decoder => ({
          partition: decoder.readInt32(),
          lowWatermark: decoder.readInt64(),
          errorCode: decoder.readInt16(),
        })),
      }))
      .sort(topicNameComparator),
  }
}

const parse = async data => {
  const topicsWithErrors = data.topics
    .map(({ partitions }) => ({
      partitionsWithErrors: partitions.filter(({ errorCode }) => failure(errorCode)),
    }))
    .filter(({ partitionsWithErrors }) => partitionsWithErrors.length)

  if (topicsWithErrors.length > 0) {
    throw createErrorFromCode(topicsWithErrors[0].partitionsWithErrors[0].errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
