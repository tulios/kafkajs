const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const { SUCCESS_CODE, KafkaProtocolError } = require('../../../error')
const response = require('./response')

describe('Protocol > Requests > ApiVersions > v0', () => {
  let decoded

  beforeEach(() => {
    decoded = {
      errorCode: 0,
      apiVersions: [
        { apiKey: apiKeys.Produce, minVersion: 0, maxVersion: 1 },
        { apiKey: apiKeys.Fetch, minVersion: 0, maxVersion: 2 },
      ],
    }
  })

  describe('response', () => {
    test('decode', () => {
      const encoded = new Encoder()
        .writeInt16(SUCCESS_CODE)
        .writeArray([
          new Encoder().writeInt16(apiKeys.Produce).writeInt16(0).writeInt16(1),
          new Encoder().writeInt16(apiKeys.Fetch).writeInt16(0).writeInt16(2),
        ])

      expect(response.decode(encoded.buffer)).toEqual(decoded)
    })

    test('parse', () => {
      expect(response.parse(decoded)).toEqual(decoded)
    })

    test('when errorCode is different than SUCCESS_CODE', () => {
      decoded.errorCode = 5
      expect(() => response.parse(decoded)).toThrowError(new KafkaProtocolError(5))
    })
  })
})
