const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * DeleteConsumerGroupOffsets Response (Version: 0) => throttle_time_ms [results]
 *   throttle_time_ms => INT32
 *   results => topic [partitions]
 *     topic => STRING
 *     partitions => partition_index error_code
 *       partition_index => INT32
 *       error_code => INT16
 */

const decodePartition = decoder => ({
  partitionIndex: decoder.readInt32(),
  errorCode: decoder.readInt16(),
})

const decodeTopic = decoder => ({
  topic: decoder.readString(),
  partitions: decoder.readArray(decodePartition),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTimeMs = decoder.readInt32()
  const results = decoder.readArray(decodeTopic)

  for (const result of results) {
    for (const partition of result.partitions) {
      if (failure(partition.errorCode)) {
        partition.error = createErrorFromCode(partition.errorCode)
      }
    }
  }
  return {
    throttleTimeMs,
    results,
  }
}

const parse = async data => {
  return data
}

module.exports = {
  decode,
  parse,
}
