const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, errorCodes } = require('../../../error')

/**
 * DescribeTransactions Response (Version: 0) => TAG_BUFFER throttle_time [transaction_states] TAG_BUFFER
 * throttle_time => INT32
 * transaction_states => error_code transactional_id state producer_id producer_epoch transaction_timeout_ms transaction_start_time_ms [topics] TAG_BUFFER
 *  error_code => INT16
 *  transactional_id => COMPACT_STRING
 *  state => COMPACT_STRING
 *  producer_id => INT64
 *  producer_epoch => INT16
 *  transaction_timeout_ms => INT32
 *  transaction_start_time_ms => INT64
 *  topics => topic [partitions] TAG_BUFFER
 *    topic => COMPACT_STRING
 *    partitions => INT32
 */

const decodePartitions = decoder => {
  return decoder.readInt32()
}

const decodeTopics = decoder => {
  const topic = {
    topic: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions),
  }

  decoder.readTaggedFields()
  return topic
}

const decodeTransactionStates = decoder => {
  const state = {
    errorCode: decoder.readInt16(),
    transactionalId: decoder.readUVarIntString(),
    state: decoder.readUVarIntString(),
    producerId: decoder.readInt64(),
    producerEpoch: decoder.readInt16(),
    transactionTimeoutMs: decoder.readInt32(),
    transactionStartTimeMs: decoder.readInt64(),
    topics: decoder.readUVarIntArray(decodeTopics),
  }

  decoder.readTaggedFields()
  return state
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  return {
    throttleTime,
    transactionStates: decoder.readUVarIntArray(decodeTransactionStates),
  }
}

const parse = async data => {
  return data
}

module.exports = {
  decode,
  parse,
}
