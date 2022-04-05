const RequestV10Protocol = require('./request')

describe('Protocol > Requests > Fetch > v10', () => {
  test('request', async () => {
    const minBytes = 1
    const maxBytes = 10485760 // 10MB
    const maxWaitTime = 100
    const maxBytesPerPartition = 1048576 // 1MB
    const topics = [
      {
        topic: 'test-topic-2077b9d2b36c4082e594-4020-b5a52b27-56df-4b87-800d-82c1cf26317d',
        partitions: [
          { partition: 0, currentLeaderEpoch: -1, fetchOffset: 0, maxBytes: maxBytesPerPartition },
        ],
      },
    ]

    const { buffer } = await RequestV10Protocol({
      replicaId: -1,
      maxWaitTime,
      minBytes,
      maxBytes,
      topics,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v10_request.json')))
  })
})
