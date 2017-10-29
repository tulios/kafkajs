const RequestV0Protocol = require('./request')

describe('Protocol > Requests > OffsetCommit > v2', () => {
  test('request', () => {
    const topic = 'test-topic-54bdab52db631498a9f8'
    const groupId = 'consumer-group-id-e5c3954d54ca5a8b75f3'
    const memberId = 'test-130128ce99f3b44190ba-adf767f2-b08f-4b99-943c-154754d768c1'
    const topics = [
      { topic, partitions: [{ partition: 0, offset: '0', timestamp: 1509292875164 }] },
    ]

    const { buffer } = RequestV0Protocol({
      groupId,
      groupGenerationId: 1,
      memberId,
      retentionTime: -1,
      topics,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v2_request.json')))
  })
})
