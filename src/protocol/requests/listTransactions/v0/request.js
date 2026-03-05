const Encoder = require('../../../encoder')
const { ListTransactions: apiKey } = require('../../apiKeys')

/**
 * ListTransactions Request (Version: 0) => TAG_BUFFER [state_filters] [producer_id_filters] TAG_BUFFER
 * state_filters => COMPACT_STRING
 * producer_id_filters => INT64
 */

module.exports = ({ stateFilters = [], producerIdFilters = [] }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ListTransactions',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(stateFilters.map(encodeStateFilter))
      .writeUVarIntArray(producerIdFilters.map(encodeProducerIdFilter))
      .writeUVarIntBytes()
  },
})

const encodeStateFilter = stateFilter => {
  return new Encoder().writeUVarIntString(stateFilter)
}

const encodeProducerIdFilter = producerId => {
  return new Encoder().writeInt64(producerId)
}
