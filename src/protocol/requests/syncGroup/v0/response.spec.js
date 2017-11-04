const { decode, parse } = require('./response')

describe('Protocol > Requests > SyncGroup > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      errorCode: 0,
      memberAssignment: Buffer.from(
        JSON.stringify({
          'topic-test': [2, 5, 4, 1, 3, 0],
        })
      ),
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
