const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * ElectLeaders Response (Version: 2) => throttle_time error_code [replica_election_results] TAG_BUFFER
 *   throttle_time => INT32
 *   error_code => INT16
 *   replica_election_results => topic [partition_result] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_result => partition_id error_code error_message TAG_BUFFER
 *       partition_id => INT32
 *       error_code => INT16
 *       error_message => COMPACT_NULLABLE_STRING
 */

const decodePartitionResult = decoder => {
  const result = {
    partitionId: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
  }
  decoder.readTaggedFields()
  return result
}

const decodeReplicaElectionResults = decoder => {
  const result = {
    topic: decoder.readUVarIntString(),
    partitionResult: decoder.readUVarIntArray(decodePartitionResult),
  }
  decoder.readTaggedFields()
  return result
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  return {
    throttleTime,
    errorCode,
    replicaElectionResults: decoder.readUVarIntArray(decodeReplicaElectionResults),
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
