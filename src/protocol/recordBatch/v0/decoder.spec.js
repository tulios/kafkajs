const Decoder = require('../../decoder')
const recordBatchDecoder = require('./decoder')

describe('Protocol > RecordBatch > v0', () => {
  test('decodes', async () => {
    const decoder = new Decoder(Buffer.from(require('../fixtures/v0_recordbatch.json')))
    const decoded = await recordBatchDecoder(decoder)

    expect(decoded).toEqual({
      firstOffset: '0',
      firstTimestamp: '1597425188775',
      partitionLeaderEpoch: 0,
      inTransaction: false,
      isControlBatch: false,
      lastOffsetDelta: 0,
      producerId: '-1',
      producerEpoch: 0,
      firstSequence: 0,
      maxTimestamp: '1597425188809',
      timestampType: 1,
      records: expect.any(Object),
    })
  })
})
