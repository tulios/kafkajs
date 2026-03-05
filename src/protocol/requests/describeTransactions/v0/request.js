const Encoder = require('../../../encoder')
const { DescribeTransactions: apiKey } = require('../../apiKeys')

/**
 * DescribeTransactions Request (Version: 0) => TAG_BUFFER [transactional_ids] TAG_BUFFER
 * transactional_ids => COMPACT_STRING
 */

module.exports = ({ transactionalIds }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeTransactions',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(transactionalIds.map(encodeTransactionalId))
      .writeUVarIntBytes()
  },
})

const encodeTransactionalId = transactionalId => {
  return new Encoder().writeUVarIntString(transactionalId)
}
