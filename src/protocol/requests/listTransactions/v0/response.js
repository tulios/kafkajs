const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, errorCodes } = require('../../../error')

/**
 * ListTransactions Response (Version: 0) => TAG_BUFFER throttle_time error_code [unknown_state_filters] [transaction_states] TAG_BUFFER
 * throttle_time => INT32
 * error_code => INT16
 * unknown_state_filters => COMPACT_STRING
 * transaction_states => transactional_id producer_id transaction_state TAG_BUFFER
 *  transactional_id => COMPACT_STRING
 *  producer_id => INT64
 *  transaction_state => COMPACT_STRING
 */

const decodeUnknownStateFilters = decoder => {
  return decoder.readUVarIntString()
}

const decodeTransactionStates = decoder => {
  const state = {
    transactionalId: decoder.readUVarIntString(),
    producerId: decoder.readInt64(),
    transactionState: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return state
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  return {
    throttleTime,
    errorCode,
    unknownStateFilters: decoder.readUVarIntArray(decodeUnknownStateFilters),
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
