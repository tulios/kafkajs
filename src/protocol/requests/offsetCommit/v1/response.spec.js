const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetCommit > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      responses: [
        { topic: 'test-topic-eb1a285cda2e9f9a1021', partitions: [{ partition: 0, errorCode: 0 }] },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
