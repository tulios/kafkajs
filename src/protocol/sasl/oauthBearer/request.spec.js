const RequestProtocol = require('./request')
const Decoder = require('../../decoder')

const SEPARATOR = ''

const decodeMessage = async request => {
  const buffer = await request.encode()
  // writeBytes prefixes an int32 length, then the raw bytes
  return new Decoder(buffer).readBytes().toString('utf8')
}

describe('Protocol > SASL > OAUTHBEARER > request', () => {
  it('encodes the message without an authorization identity', async () => {
    const request = await RequestProtocol({}, { value: 'TOKEN' })
    const message = await decodeMessage(request)
    expect(message).toEqual(`n,,${SEPARATOR}auth=Bearer TOKEN${SEPARATOR}${SEPARATOR}`)
  })

  it('encodes a valid GS2 header when an authorization identity is provided', async () => {
    const request = await RequestProtocol({ authorizationIdentity: 'admin' }, { value: 'TOKEN' })
    const message = await decodeMessage(request)
    expect(message).toEqual(`n,a=admin,${SEPARATOR}auth=Bearer TOKEN${SEPARATOR}${SEPARATOR}`)
  })
})
