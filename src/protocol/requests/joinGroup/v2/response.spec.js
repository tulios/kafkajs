const { decode, parse } = require('./response')

describe('Protocol > Requests > JoinGroup > v2', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      generationId: 1,
      groupProtocol: 'AssignerName',
      leaderId:
        'test-b773bdb220aa2b862440-23702-2b1581f6-55ea-4af0-97f0-931d4f071111-68a2051d-7b30-4161-b920-89346d7b672b',
      memberId:
        'test-b773bdb220aa2b862440-23702-2b1581f6-55ea-4af0-97f0-931d4f071111-68a2051d-7b30-4161-b920-89346d7b672b',
      members: [
        {
          memberId:
            'test-b773bdb220aa2b862440-23702-2b1581f6-55ea-4af0-97f0-931d4f071111-68a2051d-7b30-4161-b920-89346d7b672b',
          memberMetadata: Buffer.from(require('../fixtures/v2_assignerMetadata.json')),
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
