const Encoder = require('../../../encoder')
const { GroupCoordinator: apiKey } = require('../../apiKeys')

/**
 * FindCoordinator Request (Version: 3) => key key_type TAG_BUFFER
 *   key => COMPACT_STRING
 *   key_type => INT8
 */

module.exports = ({ coordinatorKey, coordinatorType }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'GroupCoordinator',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(coordinatorKey)
      .writeInt8(coordinatorType)
      .writeUVarIntBytes()
  },
})
