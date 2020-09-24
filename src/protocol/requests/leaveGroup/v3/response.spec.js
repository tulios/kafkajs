const { decode, parse } = require('./response')

describe('Protocol > Requests > LeaveGroup > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      members: [
        {
          memberId: 'test-42962f68-e801-4cd8-b359-2d862ebb4d05',
          groupInstanceId: null,
          errorCode: 0,
        },
      ],
    })
    await expect(parse(data)).resolves.toBeTruthy()
  })
})
