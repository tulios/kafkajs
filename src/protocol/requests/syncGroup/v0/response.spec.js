const { unsupportedVersionResponse } = require('testHelpers')
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

  test('handles empty member assignment', async () => {
    const data = await decode(
      Buffer.from(require('../fixtures/v0_response_empty_member_assignment.json'))
    )
    expect(data).toEqual({
      errorCode: 0,
      memberAssignment: Buffer.from([]),
    })
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponse())).rejects.toThrow(
      /The version of API is not supported/
    )
  })
})
