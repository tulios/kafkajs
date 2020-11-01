const RequestV0Protocol = require('./request')

describe('Protocol > Requests > SaslAuthenticate > v0', () => {
  describe('request', () => {
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
