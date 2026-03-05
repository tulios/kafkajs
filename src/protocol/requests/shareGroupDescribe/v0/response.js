const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

const decodePartition = decoder => {
  const partitionIndex = decoder.readInt32()
  const startOffset = decoder.readInt64()
  const stateEpoch = decoder.readInt32()
  const leaderEpoch = decoder.readInt32()
  decoder.readTaggedFields()
  return { partitionIndex, startOffset, stateEpoch, leaderEpoch }
}

const decodeTopic = decoder => {
  const topicId = decoder.readUUID()
  const topicName = decoder.readUVarIntString()
  const partitions = decoder.readUVarIntArray(decodePartition)
  decoder.readTaggedFields()
  return { topicId, topicName, partitions }
}

const decodeMember = decoder => {
  const memberId = decoder.readUVarIntString()
  const rackId = decoder.readUVarIntString()
  const memberEpoch = decoder.readInt32()
  const clientId = decoder.readUVarIntString()
  const clientHost = decoder.readUVarIntString()
  const subscribedTopicNamesLength = decoder.readUVarInt()
  const subscribedTopicNames = []
  if (subscribedTopicNamesLength > 0) {
    for (let i = 0; i < subscribedTopicNamesLength - 1; i++) {
      subscribedTopicNames.push(decoder.readUVarIntString())
    }
  }
  const assignment = decoder.readUVarIntArray(d => {
    const topicId = d.readUUID()
    const pLen = d.readUVarInt()
    const partitions = []
    if (pLen > 0) {
      for (let j = 0; j < pLen - 1; j++) {
        partitions.push(d.readInt32())
      }
    }
    d.readTaggedFields()
    return { topicId, partitions }
  })
  decoder.readTaggedFields()
  return {
    memberId,
    rackId,
    memberEpoch,
    clientId,
    clientHost,
    subscribedTopicNames,
    assignment,
  }
}

const decodeGroup = decoder => {
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const groupId = decoder.readUVarIntString()
  const groupState = decoder.readUVarIntString()
  const groupEpoch = decoder.readInt32()
  const assignmentEpoch = decoder.readInt32()
  const assignorName = decoder.readUVarIntString()
  const topics = decoder.readUVarIntArray(decodeTopic)
  const members = decoder.readUVarIntArray(decodeMember)
  const authorizedOperations = decoder.readInt32()
  decoder.readTaggedFields()
  return {
    errorCode,
    errorMessage,
    groupId,
    groupState,
    groupEpoch,
    assignmentEpoch,
    assignorName,
    topics,
    members,
    authorizedOperations,
  }
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const groups = decoder.readUVarIntArray(decodeGroup)
  return { throttleTime, groups }
}

const parse = async data => {
  for (const group of data.groups || []) {
    if (failure(group.errorCode)) {
      throw createErrorFromCode(group.errorCode)
    }
  }
  return data
}

module.exports = { decode, parse }
