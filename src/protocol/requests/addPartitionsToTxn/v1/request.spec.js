const RequestV1Protocol = require('./request')

describe('Protocol > Requests > AddPartitionsToTxn > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV1Protocol({
      transactionalId: 'test-transactional-id',
      producerId: '1001',
      producerEpoch: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [0, 1, 2, 3],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
