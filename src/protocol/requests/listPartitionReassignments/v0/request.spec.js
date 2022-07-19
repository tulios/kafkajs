const RequestV0Protocol = require('./request')

/* TO-DO: Update the fixtures, data, and errors */

describe('Protocol > Requests > AlterPartitionReassignments > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      topics: [
        {
          topic: 'test-topic-1f131dd7f83b8d72a447-33298-d13ec602-1a34-41c8-b59e-0657aef3ad25',
          partitions: [0],
        },
      ],
      timeout: 5000,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
