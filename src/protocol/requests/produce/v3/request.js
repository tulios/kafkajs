const Encoder = require('../../../encoder')
const { Produce: apiKey } = require('../../apiKeys')
const { Types } = require('../../../message/compression')
const Record = require('../../../recordBatch/record/v0')
const RecordBatch = require('../../../recordBatch/v0')

/**
 * Produce Request (Version: 3) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 */

/**
 * @param [transactionalId=null] {String} The transactional id or null if the producer is not transactional
 * @param acks {Integer} See producer request v0
 * @param timeout {Integer} See producer request v0
 * @param [compression=CompressionTypes.None] {CompressionTypes}
 * @param topicData {Array}
 */
module.exports = ({
  transactionalId = null,
  acks,
  timeout,
  compression = Types.None,
  topicData,
}) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'Produce',
  encode: async () => {
    const encodeTopic = topicEncoder(compression)
    const encodedTopicData = []

    for (let data of topicData) {
      encodedTopicData.push(await encodeTopic(data))
    }

    return new Encoder()
      .writeString(transactionalId)
      .writeInt16(acks)
      .writeInt32(timeout)
      .writeArray(encodedTopicData)
  },
})

const topicEncoder = compression => async ({ topic, partitions }) => {
  const encodePartitions = partitionsEncoder(compression)
  const encodedPartitions = []

  for (let data of partitions) {
    encodedPartitions.push(await encodePartitions(data))
  }

  return new Encoder().writeString(topic).writeArray(encodedPartitions)
}

const partitionsEncoder = compression => async ({ partition, messages }) => {
  const dateNow = Date.now()
  let timestamps = messages.map(m => m.timestamp)
  timestamps = timestamps.length === 0 ? [dateNow] : timestamps

  const firstTimestamp = Math.min(...timestamps)
  const maxTimestamp = Math.max(...timestamps)
  const records = messages.map((message, i) =>
    Record({
      ...message,
      offsetDelta: i,
      timestampDelta: (message.timestamp || dateNow) - firstTimestamp,
    })
  )

  const recordBatch = RecordBatch({
    compression,
    records,
    firstTimestamp,
    maxTimestamp,
    lastOffsetDelta: records.length - 1,
  })

  return new Encoder()
    .writeInt32(partition)
    .writeInt32(recordBatch.size())
    .writeEncoder(recordBatch)
}
