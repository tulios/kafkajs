const { decode, parse } = require('./response')
const Long = require('long')

const unsupportedVersionResponseWithTimeout = () =>
  Buffer.from({ type: 'Buffer', data: [0, 0, 0, 0, 0, 35] })

describe('Protocol > Requests > InitProducerId > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      producerId: new Long(1006, 0, false),
      producerEpoch: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponseWithTimeout())).rejects.toThrow(
      /The version of API is not supported/
    )
  })
})
