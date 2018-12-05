const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const { lookup } = require('./index')

const API_KEY_PRODUCE = 0
const API_KEY_FETCH = 1

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

  describe('Produce', () => {
    const apiKey = API_KEY_PRODUCE

    describe('when allowExperimentalV011 is enabled', () => {
      const allowExperimentalV011 = true

      it('returns the maximum supported version by the client', () => {
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 1, maxVersion: 5 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2], protocol }

        expect(lookup(versions, allowExperimentalV011)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 2 })
      })

      it('returns the new API if it is supported by client and server', () => {
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 1, maxVersion: 5 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2, 3], protocol }

        expect(lookup(versions, allowExperimentalV011)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 3 })
      })
    })

    describe('when allowExperimentalV011 is disabled', () => {
      it('returns the old API', () => {
        const allowExperimentalV011 = false
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 1, maxVersion: 5 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2, 3], protocol }

        expect(lookup(versions, allowExperimentalV011)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 2 })
      })
    })
  })

  describe('Fetch', () => {
    const apiKey = API_KEY_FETCH

    describe('when allowExperimentalV011 is enabled', () => {
      const allowExperimentalV011 = true

      it('returns the maximum supported version by the client', () => {
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 1, maxVersion: 5 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2, 3], protocol }

        expect(lookup(versions, allowExperimentalV011)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 3 })
      })

      it('returns the new API if it is supported by client and server', () => {
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 1, maxVersion: 5 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2, 3, 4], protocol }

        expect(lookup(versions, allowExperimentalV011)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 4 })
      })
    })

    describe('when allowExperimentalV011 is disabled', () => {
      it('returns the old API', () => {
        const allowExperimentalV011 = false
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 1, maxVersion: 5 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2, 3, 4], protocol }

        expect(lookup(versions, allowExperimentalV011)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 3 })
      })
    })
  })
})
