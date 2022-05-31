const response = require('./response')

describe('Protocol > Requests > DeleteRecords > v0', () => {
  test('response - success', async () => {
    const { decode, parse } = response({})
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      topics: [
        {
          topic: 'test-topic-5da683fa3b1898223498-97119-d06829e3-35d2-4b97-b4b4-7c03d4ad7cc8',
          partitions: [
            {
              partition: 0,
              lowWatermark: 7n,
              errorCode: 0,
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
