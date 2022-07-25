const RequestV0Protocol = require('./request')

describe('Protocol > Requests > AlterPartitionReassignments > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      topics: [
        {
          topic: 'test-topic-1',
          partitionAssignment: [
            { partition: 0, replicas: [0, 1] },
            { partition: 1, replicas: [1, 2] },
          ],
        },
        {
          topic: 'test-topic-2',
          partitionAssignment: [{ partition: 0, replicas: [0, 2] }],
        },
      ],
      timeout: 30000,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
