const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * ElectLeaders Response (Version: 0) => throttle_time error_code [replica_election_results]
 *   throttle_time => INT32
 *   error_code => INT16
 *   replica_election_results => topic [partition_result]
 *     topic => STRING
 *     partition_result => partition_id error_code error_message
 *       partition_id => INT32
 *       error_code => INT16
 *       error_message => NULLABLE_STRING
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    throttleTime: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    replicaElectionResults: decoder.readArray(decoder => ({
      topic: decoder.readString(),
      partitionResult: decoder.readArray(decoder => ({
        partitionId: decoder.readInt32(),
        errorCode: decoder.readInt16(),
        errorMessage: decoder.readString(),
      })),
    })),
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  for (const result of data.replicaElectionResults) {
    for (const partition of result.partitionResult) {
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
