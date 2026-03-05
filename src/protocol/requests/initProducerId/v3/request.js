const Encoder = require('../../../encoder')
const { InitProducerId: apiKey } = require('../../apiKeys')

/**
 * InitProducerId Request (Version: 3) => transactional_id transaction_timeout_ms producer_id producer_epoch TAG_BUFFER
 *   transactional_id => COMPACT_NULLABLE_STRING
 *   transaction_timeout_ms => INT32
 *   producer_id => INT64
 *   producer_epoch => INT16
 */

module.exports = ({ transactionalId, transactionTimeout, producerId, producerEpoch }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'InitProducerId',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(transactionalId)
      .writeInt32(transactionTimeout)
      .writeInt64(producerId)
      .writeInt16(producerEpoch)
      .writeUVarIntBytes()
  },
})
