const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

const decodeAcquiredRecords = decoder => {
  const firstOffset = decoder.readInt64()
  const lastOffset = decoder.readInt64()
  const deliveryCount = decoder.readInt16()
  decoder.readTaggedFields()
  return { firstOffset, lastOffset, deliveryCount }
}

const decodePartition = decoder => {
  const partitionIndex = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const currentLeader = (() => {
    const leaderId = decoder.readInt32()
    const leaderEpoch = decoder.readInt32()
    decoder.readTaggedFields()
    return { leaderId, leaderEpoch }
  })()
  const snapshotId = (() => {
    const epoch = decoder.readInt32()
    const offset = decoder.readInt64()
    decoder.readTaggedFields()
    return { epoch, offset }
  })()
  const acquiredRecords = decoder.readUVarIntArray(decodeAcquiredRecords)
  decoder.readTaggedFields()
  return { partitionIndex, errorCode, errorMessage, currentLeader, snapshotId, acquiredRecords }
}

const decodeTopic = decoder => {
  const topicId = decoder.readUUID()
  const partitions = decoder.readUVarIntArray(decodePartition)
  decoder.readTaggedFields()
  return { topicId, partitions }
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const responses = decoder.readUVarIntArray(decodeTopic)
  return { throttleTime, errorCode, errorMessage, responses }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }
  return data
}

module.exports = { decode, parse }
