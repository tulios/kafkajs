const RequestV0Protocol = require('../v0/request')

describe('Protocol > Requests > Fetch > v0', () => {
  test('request', async () => {
    const minBytes = 1
    const maxWaitTime = 5
    const maxBytes = 1048576 // 1MB
    const topics = [
      {
        topic: 'test-topic',
        partitions: [
          {
            partition: 0,
            fetchOffset: 0,
            maxBytes,
          },
        ],
      },
    ]

    const { buffer } = await RequestV0Protocol({ maxWaitTime, minBytes, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
