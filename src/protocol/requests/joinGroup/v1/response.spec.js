const { unsupportedVersionResponse } = require('testHelpers')
const { decode, parse } = require('./response')

describe('Protocol > Requests > JoinGroup > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      errorCode: 0,
      generationId: 1,
      groupProtocol: 'AssignerName',
      leaderId:
        'test-9dfcfa24752a27f51026-21486-91f3d0e3-dc7a-464f-ab89-94cb49a53a62-d9c2c2a5-cb2f-468e-a943-368336a2cd48',
      memberId:
        'test-9dfcfa24752a27f51026-21486-91f3d0e3-dc7a-464f-ab89-94cb49a53a62-d9c2c2a5-cb2f-468e-a943-368336a2cd48',
      members: [
        {
          memberId:
            'test-9dfcfa24752a27f51026-21486-91f3d0e3-dc7a-464f-ab89-94cb49a53a62-d9c2c2a5-cb2f-468e-a943-368336a2cd48',
          memberMetadata: Buffer.from(require('../fixtures/v1_assignerMetadata.json')),
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponse())).rejects.toThrow(
      /The version of API is not supported/
    )
  })
})
