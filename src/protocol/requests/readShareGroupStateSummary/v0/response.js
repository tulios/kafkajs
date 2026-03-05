const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

const decodePartition = decoder => {
  const partitionIndex = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const stateEpoch = decoder.readInt32()
  const startOffset = decoder.readInt64()
  decoder.readTaggedFields()
  return { partitionIndex, errorCode, errorMessage, stateEpoch, startOffset }
}

const decodeTopic = decoder => {
  const groupId = decoder.readUVarIntString()
  const topicId = decoder.readUUID()
  const partitions = decoder.readUVarIntArray(decodePartition)
  decoder.readTaggedFields()
  return { groupId, topicId, partitions }
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const results = decoder.readUVarIntArray(decodeTopic)
  return { throttleTime, results }
}

const parse = async data => {
  for (const result of data.results || []) {
    for (const partition of result.partitions || []) {
      if (failure(partition.errorCode)) {
        throw createErrorFromCode(partition.errorCode)
      }
    }
  }
  return data
}

module.exports = { decode, parse }
