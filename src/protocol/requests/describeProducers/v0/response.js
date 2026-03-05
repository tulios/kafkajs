const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, errorCodes } = require('../../../error')

/**
 * DescribeProducers Response (Version: 0) => TAG_BUFFER throttle_time [topics] TAG_BUFFER
 * throttle_time => INT32
 * topics => name [partitions] TAG_BUFFER
 *  name => COMPACT_STRING
 *  partitions => partition_index error_code error_message [active_producers] TAG_BUFFER
 *    partition_index => INT32
 *    error_code => INT16
 *    error_message => COMPACT_NULLABLE_STRING
 *    active_producers => producer_id producer_epoch last_sequence last_timestamp coordinator_epoch current_txn_start_offset TAG_BUFFER
 *      producer_id => INT64
 *      producer_epoch => INT32
 *      last_sequence => INT32
 *      last_timestamp => INT64
 *      coordinator_epoch => INT32
 *      current_txn_start_offset => INT64
 */

const decodeActiveProducers = decoder => {
  const producer = {
    producerId: decoder.readInt64(),
    producerEpoch: decoder.readInt32(),
    lastSequence: decoder.readInt32(),
    lastTimestamp: decoder.readInt64(),
    coordinatorEpoch: decoder.readInt32(),
    currentTxnStartOffset: decoder.readInt64(),
  }

  decoder.readTaggedFields()
  return producer
}

const decodePartitions = decoder => {
  const partition = {
    partitionIndex: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    activeProducers: decoder.readUVarIntArray(decodeActiveProducers),
  }

  decoder.readTaggedFields()
  return partition
}

const decodeTopics = decoder => {
  const topic = {
    name: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions),
  }

  decoder.readTaggedFields()
  return topic
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  return {
    throttleTime,
    topics: decoder.readUVarIntArray(decodeTopics),
  }
}

const parse = async data => {
  return data
}

module.exports = {
  decode,
  parse,
}
