const { decode, parse } = require('./response')

describe('Protocol > Requests > SyncGroup > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      errorCode: 0,
      memberAssignment: Buffer.from(require('../fixtures/v1_memberAssignment.json')),
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
