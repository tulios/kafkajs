const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const { lookup } = require('./index')

describe('Protocol > Requests > lookup', () => {
  describe('when the client support more versions than the server', () => {
    it('returns the maximum version supported by the server', async () => {
      const apiKey = 1
      const protocol = jest.fn(() => true)

      // versions supported by the server
      const versions = { [apiKey]: { minVersion: 0, maxVersion: 1 } }

      // versions supported by the client
      const definition = { versions: [0, 1, 2], protocol }

      expect(lookup(versions)(apiKey, definition)).toEqual(true)
      expect(protocol).toHaveBeenCalledWith({ version: 1 })
    })
  })

  describe('when the server support more versions than the client', () => {
    it('returns the maximum version supported by the client', () => {
      const apiKey = 1
      const protocol = jest.fn(() => true)

      // versions supported by the server
      const versions = { [apiKey]: { minVersion: 1, maxVersion: 3 } }

      // versions supported by the client
      const definition = { versions: [0, 1, 2], protocol }

      expect(lookup(versions)(apiKey, definition)).toEqual(true)
      expect(protocol).toHaveBeenCalledWith({ version: 2 })
    })
  })

  describe('when the server does not support the requested version', () => {
    it('throws KafkaJSServerDoesNotSupportApiKey', () => {
      // versions supported by the server
      const versions = { 1: { minVersion: 1, maxVersion: 3 } }

      // versions supported by the client
      const protocol = jest.fn(() => true)
      const definition = { versions: [0, 1, 2], protocol }

      const apiKeyNotSupportedByTheServer = 34
      expect(() => lookup(versions)(apiKeyNotSupportedByTheServer, definition)).toThrow(
        KafkaJSServerDoesNotSupportApiKey
      )
    })
  })
})
