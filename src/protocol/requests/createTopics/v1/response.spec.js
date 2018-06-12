const { decode, parse } = require('./response')

describe('Protocol > Requests > CreateTopics > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      topicErrors: [
        {
          topic: 'test-topic-98abe97d412df76b4deb',
          errorCode: 0,
          errorMessage: null,
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
