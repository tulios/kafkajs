const RequestV1Protocol = require('./request')

describe('Protocol > Requests > OffsetFetch > v1', () => {
  test('request', async () => {
    const groupId = 'consumer-group-id-c7dcb2473b6a1196b2b2'
    const topics = [
      {
        topic: 'test-topic-9f9b074057acd4335946',
        partitions: [{ partition: 0 }],
      },
    ]

    const { buffer } = await RequestV1Protocol({ groupId, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
