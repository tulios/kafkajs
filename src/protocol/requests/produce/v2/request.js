const Encoder = require('../../../encoder')
const { Produce: apiKey } = require('../../apiKeys')
const MessageSet = require('../../../messageSet')

// Produce Request on or after v2 indicates the client can parse the timestamp field
// in the produce Response.

module.exports = ({ acks, timeout, topicData }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'Produce',
  encode: () => {
    return new Encoder().writeInt16(acks).writeInt32(timeout).writeArray(topicData.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartitions))
}

const encodePartitions = ({ partition, messages }) => {
  const messageSet = MessageSet({ messageVersion: 1, entries: messages })
  return new Encoder().writeInt32(partition).writeInt32(messageSet.size()).writeEncoder(messageSet)
}
