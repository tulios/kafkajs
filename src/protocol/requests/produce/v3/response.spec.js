const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      topics: [
        {
          partitions: [{ baseOffset: '0', errorCode: 0, logAppendTime: '-1', partition: 0 }],
          topicName: 'test-topic-ebba68879c6f5081d8c2',
        },
      ],
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
