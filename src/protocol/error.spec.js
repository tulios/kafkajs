const { errorCodes, createErrorFromCode } = require('./error')

describe('Protocol > error', () => {
  describe('#createErrorFromCode', () => {
    it('creates enhanced errors based on kafka error codes', () => {
      for (let errorCode of errorCodes) {
        const error = createErrorFromCode(errorCode.code)
        expect(error).toHaveProperty('message', errorCode.message)
        expect(error).toHaveProperty('type', errorCode.type)
        expect(error).toHaveProperty('code', errorCode.code)
        expect(error).toHaveProperty('retriable', errorCode.retriable)
      }
    })

    it('has a fallback error in case the error code is not supported', () => {
      const error = createErrorFromCode(123456789)
      expect(error).toBeTruthy()
      expect(error).toHaveProperty('type', 'KAFKAJS_UNKNOWN_ERROR_CODE')
      expect(error).toHaveProperty('code', -99)
      expect(error).toHaveProperty('retriable', false)
      expect(error).toHaveProperty('message', 'Unknown error code 123456789')
    })
  })
})
