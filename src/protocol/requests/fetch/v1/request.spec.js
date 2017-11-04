const RequestV1Protocol = require('../v1/request')

describe('Protocol > Requests > Fetch > v1', () => {
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

    const { buffer } = await RequestV1Protocol({ maxWaitTime, minBytes, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
