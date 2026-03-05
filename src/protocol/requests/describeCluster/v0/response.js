const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, errorCodes } = require('../../../error')

/**
 * DescribeCluster Response (Version: 0) => TAG_BUFFER throttle_time error_code error_message cluster_id controller_id [brokers] cluster_authorized_operations TAG_BUFFER
 *   throttle_time => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   cluster_id => COMPACT_STRING
 *   controller_id => INT32
 *   brokers => broker_id host port rack TAG_BUFFER
 *     broker_id => INT32
 *     host => COMPACT_STRING
 *     port => INT32
 *     rack => COMPACT_NULLABLE_STRING
 *   cluster_authorized_operations => INT32
 */

const decodeBrokers = decoder => {
  const broker = {
    brokerId: decoder.readInt32(),
    host: decoder.readUVarIntString(),
    port: decoder.readInt32(),
    rack: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return broker
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const clusterId = decoder.readUVarIntString()
  const controllerId = decoder.readInt32()
  const brokers = decoder.readUVarIntArray(decodeBrokers)
  const clusterAuthorizedOperations = decoder.readInt32()

  return {
    throttleTime,
    errorCode,
    errorMessage,
    clusterId,
    controllerId,
    brokers,
    clusterAuthorizedOperations,
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
