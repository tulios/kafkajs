const Encoder = require('../../../encoder')
const { InitProducerId: apiKey } = require('../../apiKeys')

/**
 * InitProducerId Request (Version: 2) => transactional_id transaction_timeout_ms TAG_BUFFER
 *   transactional_id => COMPACT_NULLABLE_STRING
 *   transaction_timeout_ms => INT32
 */

module.exports = ({ transactionalId, transactionTimeout }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'InitProducerId',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(transactionalId)
      .writeInt32(transactionTimeout)
      .writeUVarIntBytes()
  },
})
