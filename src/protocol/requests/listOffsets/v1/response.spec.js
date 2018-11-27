const { decode, parse } = require('./response')

describe('Protocol > Requests > ListOffsets > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-16e956902e39874d06f5-91705-2958a472-e582-47a4-86f0-b258630fb3e6',
          partitions: [{ partition: 0, errorCode: 0, timestamp: '1543343103774', offset: '0' }],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
