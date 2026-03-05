const Encoder = require('../../../encoder')
const { DescribeCluster: apiKey } = require('../../apiKeys')

/**
 * DescribeCluster Request (Version: 0) => include_cluster_authorized_operations TAG_BUFFER
 *   include_cluster_authorized_operations => BOOLEAN
 */

module.exports = ({ includeClusterAuthorizedOperations = false }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeCluster',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeBoolean(includeClusterAuthorizedOperations)
      .writeUVarIntBytes()
  },
})
