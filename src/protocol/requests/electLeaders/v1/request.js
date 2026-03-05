const requestV0 = require('../v0/request')

/**
 * ElectLeaders Request (Version: 1) => election_type [topic_partitions] timeout
 *   election_type => INT8
 *   topic_partitions => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 *   timeout => INT32
 */
module.exports = ({ electionType, topicPartitions, timeout }) =>
  Object.assign(requestV0({ electionType, topicPartitions, timeout }), { apiVersion: 1 })
