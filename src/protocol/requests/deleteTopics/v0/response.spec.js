const { decode, parse } = require('./response')

describe('Protocol > Requests > DeleteTopics > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      topicErrors: [
        { topic: 'test-topic-bb9886eb859786ce646d', errorCode: 0 },
        { topic: 'test-topic-d41416601b422429db78', errorCode: 0 },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
