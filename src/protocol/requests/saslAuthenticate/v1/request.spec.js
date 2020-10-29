const RequestV1Protocol = require('./request')

describe('Protocol > Requests > SaslAuthenticate > v1', () => {
  describe('request', () => {
    describe('PLAIN', () => {
      test('encode', async () => {
        const { buffer } = await RequestV1Protocol({
          authBytes: Buffer.from(require('../fixtures/plain_bytes.json')),
        }).encode()

        expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request_plain.json')))
      })
    })

    describe('SCRAM', () => {
      test('encode', async () => {
        const { buffer } = await RequestV1Protocol({
          authBytes: Buffer.from(require('../fixtures/scram256_bytes.json')),
        }).encode()

        expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request_scram256.json')))
      })
    })
  })
})
