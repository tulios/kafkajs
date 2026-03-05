const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

const decodeAssignment = decoder => {
  const topicId = decoder.readUUID()
  const length = decoder.readUVarInt()
  const partitions = []
  if (length > 0) {
    for (let i = 0; i < length - 1; i++) {
      partitions.push(decoder.readInt32())
    }
  }
  decoder.readTaggedFields()
  return { topicId, partitions }
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const memberId = decoder.readUVarIntString()
  const memberEpoch = decoder.readInt32()
  const heartbeatIntervalMs = decoder.readInt32()
  const assignment = decoder.readUVarIntArray(decodeAssignment)

  return {
    throttleTime,
    errorCode,
    errorMessage,
    memberId,
    memberEpoch,
    heartbeatIntervalMs,
    assignment,
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }
  return data
}

module.exports = { decode, parse }
