const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetDelete > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      errorCode: 0,
      throttleTime: 0,
      topics: [
        {
          topic: 'bar',
          partitions: [
            {
              errorCode: 0,
              partition: 0,
            },
            {
              errorCode: 0,
              partition: 1,
            },
            {
              errorCode: 0,
              partition: 2,
            },
            {
              errorCode: 0,
              partition: 3,
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
