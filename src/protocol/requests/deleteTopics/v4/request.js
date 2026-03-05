const Encoder = require('../../../encoder')
const { DeleteTopics: apiKey } = require('../../apiKeys')

/**
 * DeleteTopics Request (Version: 4) => [topics] timeout_ms TAG_BUFFER
 *   topics => name TAG_BUFFER
 *     name => COMPACT_NULLABLE_STRING
 *   timeout_ms => INT32
 */

module.exports = ({ topics, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'DeleteTopics',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeInt32(timeout)
      .writeUVarIntBytes()
  },
})

const encodeTopic = topic => {
  return new Encoder().writeUVarIntString(topic).writeUVarIntBytes()
}
