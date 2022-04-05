const RequestV1Protocol = require('./request')

describe('Protocol > Requests > AddOffsetsToTxn > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV1Protocol({
      transactionalId: 'test-transactional-id',
      producerId: '1001',
      producerEpoch: 0,
      groupId: 'foobar',
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
