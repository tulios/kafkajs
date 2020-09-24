const { decode, parse } = require('./response')

describe('Protocol > Requests > SyncGroup > v2', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      memberAssignment: Buffer.from(require('../fixtures/v1_memberAssignment.json')),
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
