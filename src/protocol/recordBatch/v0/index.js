const Long = require('long')
const Encoder = require('../../encoder')
const crc32C = require('../crc32C')
const { Types: Compression } = require('../../message/compression')

const MAGIC_BYTE = 2

/**
 * v0
 * RecordBatch =>
 *  FirstOffset => int64
 *  Length => int32
 *  PartitionLeaderEpoch => int32
 *  Magic => int8
 *  CRC => int32
 *  Attributes => int16
 *  LastOffsetDelta => int32
 *  FirstTimestamp => int64
 *  MaxTimestamp => int64
 *  ProducerId => int64
 *  ProducerEpoch => int16
 *  FirstSequence => int32
 *  Records => [Record]
 */

const RecordBatch = ({
  compression = Compression.None,
  firstOffset = Long.fromInt(0),
  firstTimestamp = Date.now(),
  maxTimestamp = Date.now(),
  partitionLeaderEpoch = 0,
  lastOffsetDelta = 0,
  producerId = Long.fromValue(-1), // for idempotent messages
  producerEpoch = 0, // for idempotent messages
  firstSequence = 0, // for idempotent messages
  records = [],
}) => {
  const batchBody = new Encoder()
    .writeInt16(0) // TODO: attributes - add codec + others
    .writeInt32(lastOffsetDelta)
    .writeInt64(firstTimestamp)
    .writeInt64(maxTimestamp)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeInt32(firstSequence)
    .writeArray(records)

  // CRC32C validation is happening here:
  // https://github.com/apache/kafka/blob/0.11.0.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L148

  const batch = new Encoder()
    .writeInt32(partitionLeaderEpoch)
    .writeInt8(MAGIC_BYTE)
    .writeUInt32(crc32C(batchBody.buffer))
    .writeEncoder(batchBody)

  return new Encoder().writeInt64(firstOffset).writeBytes(batch.buffer)
}

module.exports = {
  RecordBatch,
  MAGIC_BYTE,
}
