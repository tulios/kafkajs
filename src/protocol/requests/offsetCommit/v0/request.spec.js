const RequestV0Protocol = require('./request')

describe('Protocol > Requests > OffsetCommit > v0', () => {
  test('request', () => {
    const topic = 'test-topic-eb1a285cda2e9f9a1021'
    const groupId = 'consumer-group-id-9ea5b85471316d2753ab'
    const topics = [{ topic, partitions: [{ partition: 0, offset: '0' }] }]

    const { buffer } = RequestV0Protocol({ groupId, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
