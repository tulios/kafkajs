const { decode, parse } = require('./response')

describe('Protocol > Requests > AlterPartitionReassignments > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 500,
      errorCode: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 0, errorCode: 0 },
            { partition: 1, errorCode: 0 },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
