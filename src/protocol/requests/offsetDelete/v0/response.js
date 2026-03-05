const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * OffsetDelete Response (Version: 0) => error_code throttle_time [topics]
 *   error_code => INT16
 *   throttle_time => INT32
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index error_code
 *       partition_index => INT32
 *       error_code => INT16
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    errorCode: decoder.readInt16(),
    throttleTime: decoder.readInt32(),
    topics: decoder.readArray(decoder => ({
      topic: decoder.readString(),
      partitions: decoder.readArray(decoder => ({
        partition: decoder.readInt32(),
        errorCode: decoder.readInt16(),
      })),
    })),
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  for (const topic of data.topics) {
    for (const partition of topic.partitions) {
      if (failure(partition.errorCode)) {
        throw createErrorFromCode(partition.errorCode)
      }
    }
  }

  return data
}

module.exports = {
  decode,
  parse,
}
