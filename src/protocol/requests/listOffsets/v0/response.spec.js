const { decode, parse } = require('./response')

describe('Protocol > Requests > Offsets > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-727705ce68c29fedddf4',
          partitions: [{ partition: 0, errorCode: 0, offsets: ['0'] }],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
