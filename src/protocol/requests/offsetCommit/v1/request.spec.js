const RequestV0Protocol = require('./request')

describe('Protocol > Requests > OffsetCommit > v1', () => {
  test('request', async () => {
    const topic = 'test-topic-9c1581c756889e8773dd'
    const groupId = 'consumer-group-id-25c9a1474733b283e6c6'
    const memberId = 'test-d001f2e7c1d704ed30f7-1cf32daa-64e3-4305-a0a5-db4088dfb4eb'
    const topics = [
      { topic, partitions: [{ partition: 0, offset: '0', timestamp: 1509292875164 }] },
    ]

    const { buffer } = await RequestV0Protocol({
      groupId,
      groupGenerationId: 1,
      memberId,
      topics,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })

  test('request with metadata', async () => {
    const topic = 'test-topic-9c1581c756889e8773dd'
    const groupId = 'consumer-group-id-25c9a1474733b283e6c6'
    const memberId = 'test-d001f2e7c1d704ed30f7-1cf32daa-64e3-4305-a0a5-db4088dfb4eb'
    const topics = [
      {
        topic,
        partitions: [{ partition: 0, offset: '0', timestamp: 1509292875164, metadata: 'test' }],
      },
    ]

    const { buffer } = await RequestV0Protocol({
      groupId,
      groupGenerationId: 1,
      memberId,
      topics,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request_metadata.json')))
  })
})
