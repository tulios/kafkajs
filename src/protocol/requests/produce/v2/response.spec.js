const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v2', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-919fb44e912ac0dc2693',
          partitions: [{ partition: 0, errorCode: 0, offset: '3', timestamp: '-1' }],
        },
      ],
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('response with GZIP', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response_gzip.json')))
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-bc674c30572e8ded886a',
          partitions: [{ partition: 0, errorCode: 0, offset: '3', timestamp: '-1' }],
        },
      ],
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
