const Decoder = require('../../decoder')
const { lookupCodecByRecordBatchAttributes } = require('../../message/compression')
const { KafkaJSPartialMessageError } = require('../../../errors')
const RecordDecoder = require('../record/v0/decoder')

const TRANSACTIONAL_FLAG_MASK = 0x10
const CONTROL_FLAG_MASK = 0x20

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

module.exports = async decoder => {
  const firstOffset = decoder.readInt64().toString()
  const length = decoder.readInt32()

  const remainingBytes = Buffer.byteLength(decoder.slice(length).buffer)

  if (remainingBytes < length) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial record batch: remainingBytes(${remainingBytes}) < recordBatchLength(${length})`
    )
  }

  const partitionLeaderEpoch = decoder.readInt32()

  // The magic byte was read by the Fetch protocol to distinguish between
  // the record batch and the legacy message set. It's not used here but
  // it has to be read.
  const magicByte = decoder.readInt8() // eslint-disable-line no-unused-vars

  // The library is currently not performing CRC validations
  const crc = decoder.readInt32() // eslint-disable-line no-unused-vars

  const attributes = decoder.readInt16()
  const lastOffsetDelta = decoder.readInt32()
  const firstTimestamp = decoder.readInt64().toString()
  const maxTimestamp = decoder.readInt64().toString()
  const producerId = decoder.readInt64().toString()
  const producerEpoch = decoder.readInt16()
  const firstSequence = decoder.readInt32()

  const inTransaction = (attributes & TRANSACTIONAL_FLAG_MASK) > 0
  const isControlBatch = (attributes & CONTROL_FLAG_MASK) > 0
  const codec = lookupCodecByRecordBatchAttributes(attributes)

  const recordsSize = Buffer.byteLength(decoder.buffer)
  const recordsDecoder = decoder.slice(recordsSize)
  const recordContext = { firstOffset, firstTimestamp, magicByte }
  const records = await decodeRecords(codec, recordsDecoder, recordContext)

  return {
    firstOffset,
    firstTimestamp,
    partitionLeaderEpoch,
    inTransaction,
    isControlBatch,
    lastOffsetDelta,
    producerId,
    producerEpoch,
    firstSequence,
    maxTimestamp,
    records,
  }
}

const decodeRecords = async (codec, recordsDecoder, recordContext) => {
  if (!codec) {
    return recordsDecoder.readArray(decoder => decodeRecord(decoder, recordContext))
  }

  const length = recordsDecoder.readInt32()

  if (length === -1) {
    return []
  }

  const compressedRecordsBuffer = recordsDecoder.readAll()
  const decompressedRecordBuffer = await codec.decompress(compressedRecordsBuffer)
  const decompressedRecordDecoder = new Decoder(decompressedRecordBuffer)
  const records = []

  for (let i = 0; i < length; i++) {
    records.push(decodeRecord(decompressedRecordDecoder, recordContext))
  }

  return records
}

const decodeRecord = (decoder, recordContext) => {
  const recordBuffer = decoder.readVarIntBytes()
  return RecordDecoder(new Decoder(recordBuffer), recordContext)
}
