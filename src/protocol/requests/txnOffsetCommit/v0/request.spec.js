const RequestV0Protocol = require('./request')

describe('Protocol > Requests > TxnOffsetCommit > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      transactionalId: 'test-transactional-id',
      groupId: 'test-group-id',
      producerId: 20000,
      producerEpoch: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 1, offset: 0 },
            { partition: 2, offset: 0 },
          ],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
