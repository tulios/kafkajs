const RequestV4Protocol = require('./request')

describe('Protocol > Requests > Fetch > v4', () => {
  test('request', async () => {
    const minBytes = 1
    const maxBytes = 10485760 // 10MB
    const maxWaitTime = 5
    const maxBytesPerPartition = 1048576 // 1MB
    const topics = [
      {
        topic: 'test-topic',
        partitions: [
          {
            partition: 0,
            fetchOffset: 0,
            maxBytes: maxBytesPerPartition,
          },
        ],
      },
    ]

    const { buffer } = await RequestV4Protocol({ maxWaitTime, minBytes, maxBytes, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v4_request.json')))
  })
})
