const { decode, parse } = require('./response')

describe('Protocol > Requests > AlterPartitionReassignments > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      topics: [
        {
          name: 'test-topic-1f131dd7f83b8d72a447-33298-d13ec602-1a34-41c8-b59e-0657aef3ad25',
          partitions: [
            { partition: 0, replicas: [2, 1, 0], addingReplicas: [1], removingReplicas: [0] },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
