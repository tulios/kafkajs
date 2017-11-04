const RequestV3Protocol = require('./request')

describe('Protocol > Requests > Fetch > v3', () => {
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

    const { buffer } = await RequestV3Protocol({ maxWaitTime, minBytes, maxBytes, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request.json')))
  })
})
