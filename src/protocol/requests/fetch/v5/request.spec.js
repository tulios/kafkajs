const RequestV5Protocol = require('./request')

describe('Protocol > Requests > Fetch > v5', () => {
  test('request', async () => {
    const minBytes = 1
    const maxBytes = 10485760 // 10MB
    const maxWaitTime = 100
    const maxBytesPerPartition = 1048576 // 1MB
    const topics = [
      {
        topic: 'test-topic-c935d678835de2c9c79e-2064-677041b7-df54-4d4d-a53a-b9133d2fdc8c',
        partitions: [{ partition: 0, fetchOffset: 0, maxBytes: maxBytesPerPartition }],
      },
    ]

    const { buffer } = await RequestV5Protocol({
      replicaId: -1,
      maxWaitTime,
      minBytes,
      maxBytes,
      topics,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v5_request.json')))
  })
})
