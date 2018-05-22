const Long = require('long')
const Encoder = require('../../encoder')
const crc32 = require('../../crc32')
const { Types: Compression } = require('../../message/compression')

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

module.exports = ({
  compression = Compression.None,
  firstOffset = Long.fromInt(0),
  firstTimestamp = Date.now(),
  maxTimestamp = Date.now(),
  partitionLeaderEpoch = 0,
  lastOffsetDelta = 0,
  producerId = -1, // for idempotent messages
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

  const batch = new Encoder()
    .writeInt32(partitionLeaderEpoch)
    .writeInt8(2) // magicByte
    .writeInt32(crc32(batchBody))
    .writeEncoder(batchBody)

  return new Encoder().writeInt64(firstOffset).writeBytes(batch.buffer)
}
