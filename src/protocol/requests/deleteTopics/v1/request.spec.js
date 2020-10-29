const RequestV1Protocol = require('./request')

describe('Protocol > Requests > DeleteTopics > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV1Protocol({
      topics: [
        'test-topic-386ea404396d663a8042-56298-e6e26331-de25-48d8-90b6-4710cd0b618b',
        'test-topic-bb5d4c0c37ae53eb8b53-56298-ac202bf8-78e7-4d8b-ad07-4e01d8148db0',
      ],
      timeout: 5000,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
