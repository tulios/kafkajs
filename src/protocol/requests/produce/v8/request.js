const requestV7 = require('../v7/request')

/**
 * Produce Request (Version: 8) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 */

module.exports = ({
  acks,
  timeout,
  transactionalId,
  producerId,
  producerEpoch,
  compression,
  topicData,
}) =>
  Object.assign(
    requestV7({
      acks,
      timeout,
      transactionalId,
      producerId,
      producerEpoch,
      compression,
      topicData,
    }),
    { apiVersion: 8 }
  )
