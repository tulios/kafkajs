const Encoder = require('../../../encoder')
const { DescribeCluster: apiKey } = require('../../apiKeys')

/**
 * DescribeCluster Request (Version: 1) => include_cluster_authorized_operations endpoint_type TAG_BUFFER
 *   include_cluster_authorized_operations => BOOLEAN
 *   endpoint_type => INT8
 */

module.exports = ({ includeClusterAuthorizedOperations = false, endpointType = 1 }) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'DescribeCluster',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeBoolean(includeClusterAuthorizedOperations)
      .writeInt8(endpointType)
      .writeUVarIntBytes()
  },
})
