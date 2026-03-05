const Decoder = require('../../../decoder')
const { parse: parseV1 } = require('../v1/response')
const decodeMessages = require('./decodeMessages')

/**
 * Fetch Response (Version: 12) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   session_id => INT32
 *   responses => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
 *       partition => INT32
 *       error_code => INT16
 *       high_watermark => INT64
 *       last_stable_offset => INT64
 *       log_start_offset => INT64
 *       aborted_transactions => producer_id first_offset TAG_BUFFER
 *         producer_id => INT64
 *         first_offset => INT64
 *       preferred_read_replica => INT32
 *       records => COMPACT_RECORDS
 */

const decodeAbortedTransactions = decoder => {
  const abortedTransaction = {
    producerId: decoder.readInt64().toString(),
    firstOffset: decoder.readInt64().toString(),
  }
  decoder.readTaggedFields()
  return abortedTransaction
}

const decodeAbortedTransactionsArray = decoder => {
  const length = decoder.readUVarInt()

  if (length === 0) {
    return null
  }

  const array = new Array(length - 1)
  for (let i = 0; i < length - 1; i++) {
    array[i] = decodeAbortedTransactions(decoder)
  }

  return array
}

const decodePartition = async decoder => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    highWatermark: decoder.readInt64().toString(),
    lastStableOffset: decoder.readInt64().toString(),
    lastStartOffset: decoder.readInt64().toString(),
    abortedTransactions: decodeAbortedTransactionsArray(decoder),
    preferredReadReplica: decoder.readInt32(),
    messages: await decodeMessages(decoder),
  }
  decoder.readTaggedFields()
  return partition
}

const decodeResponse = async decoder => {
  const topicName = decoder.readUVarIntString()
  const length = decoder.readUVarInt()
  const partitions = []

  for (let i = 0; i < length - 1; i++) {
    partitions.push(await decodePartition(decoder))
  }

  decoder.readTaggedFields()
  return { topicName, partitions }
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const clientSideThrottleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const sessionId = decoder.readInt32()

  const length = decoder.readUVarInt()
  const responses = []

  for (let i = 0; i < length - 1; i++) {
    responses.push(await decodeResponse(decoder))
  }

  decoder.readTaggedFields()

  // Report a `throttleTime` of 0: The broker will not have throttled
  // this request, but if the `clientSideThrottleTime` is >0 then it
  // expects us to do that -- and it will ignore requests.
  return {
    throttleTime: 0,
    clientSideThrottleTime,
    errorCode,
    sessionId,
    responses,
  }
}

module.exports = {
  decode,
  parse: parseV1,
}
