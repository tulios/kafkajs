const { decode, parse } = require('./response')

describe('Protocol > Requests > JoinGroup > v0', () => {
  test('response', () => {
    const data = decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      errorCode: 0,
      generationId: 11,
      groupProtocol: 'default',
      leaderId: 'test-169029db29f2ebfe07c1-fe0d5338-804e-42fa-af6a-c8f7b2467c6e',
      memberId: 'test-169029db29f2ebfe07c1-fe0d5338-804e-42fa-af6a-c8f7b2467c6e',
      members: [
        {
          memberId: 'test-169029db29f2ebfe07c1-fe0d5338-804e-42fa-af6a-c8f7b2467c6e',
          memberMetadata: Buffer.from({ type: 'Buffer', data: [0, 0] }),
        },
      ],
    })

    expect(() => parse(data)).not.toThrowError()
  })
})
