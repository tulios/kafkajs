const RequestV1Protocol = require('./request')

describe('Protocol > Requests > ListOffsets > v1', () => {
  test('request', async () => {
    const timestamp = 1509285569484
    const topics = [
      {
        topic: 'test-topic-173c0e1556dab8d50ee6-91677-379faf0f-a357-408e-bd1d-5fa11893b05d',
        partitions: [{ partition: 0, timestamp }],
      },
    ]

    const { buffer } = await RequestV1Protocol({ replicaId: -1, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
