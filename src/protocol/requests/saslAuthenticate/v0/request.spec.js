const apiKeys = require('../../apiKeys')
const RequestV0Protocol = require('./request')

describe('Protocol > Requests > SaslAuthenticate > v0', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV0Protocol({ authBytes: Buffer.alloc(0) })
      expect(request.apiKey).toEqual(apiKeys.SaslAuthenticate)
      expect(request.apiVersion).toEqual(0)
      expect(request.apiName).toEqual('SaslAuthenticate')
    })

    describe('PLAIN', () => {
      test('encode', async () => {
        const { buffer } = await RequestV0Protocol({
          authBytes: Buffer.from(require('../fixtures/plain_bytes.json')),
        }).encode()

        expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request_plain.json')))
      })
    })

    describe('SCRAM', () => {
      test('encode', async () => {
        const { buffer } = await RequestV0Protocol({
          authBytes: Buffer.from(require('../fixtures/scram256_bytes.json')),
        }).encode()

        expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request_scram256.json')))
      })
    })
  })
})
