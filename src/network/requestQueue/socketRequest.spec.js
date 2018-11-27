const sleep = require('../../utils/sleep')
const SocketRequest = require('./socketRequest')
const { KafkaJSRequestTimeoutError, KafkaJSNonRetriableError } = require('../../errors')

describe('Network > SocketRequest', () => {
  let request, sendRequest, timeoutHandler
  let correlationId = 0
  const requestTimeout = 50
  const size = 32
  const payload = { ok: true }

  beforeEach(() => {
    sendRequest = jest.fn()
    timeoutHandler = jest.fn()
    request = new SocketRequest({
      requestTimeout,
      broker: 'localhost:9092',
      expectResponse: true,
      entry: { correlationId: correlationId++, resolve: jest.fn(), reject: jest.fn() },
      send: sendRequest,
    })

    request.onTimeout(timeoutHandler)
  })

  describe('#onTimeout', () => {
    it('assigns the timeoutHandler', () => {
      const request = new SocketRequest({ entry: {} })
      const handler = jest.fn()
      request.onTimeout(handler)
      expect(request.timeoutHandler).toEqual(handler)
    })
  })

  describe('#send', () => {
    it('sends the request using the provided function', () => {
      expect(request.sentAt).toEqual(null)
      expect(request.pendingDuration).toEqual(null)
      expect(request.timeoutId).toEqual(null)

      request.send()

      expect(sendRequest).toHaveBeenCalled()
      expect(request.sentAt).toEqual(expect.any(Number))
      expect(request.pendingDuration).toEqual(expect.any(Number))
      expect(request.timeoutId).not.toEqual(null)

      clearTimeout(request.timeoutId)
    })

    it('does not call sendRequest more than once', () => {
      request.send()
      expect(() => request.send()).toThrow(KafkaJSNonRetriableError)

      expect(sendRequest).toHaveBeenCalledTimes(1)
      clearTimeout(request.timeoutId)
    })

    it('executes the timeoutHandler when it times out', async () => {
      jest.spyOn(request, 'rejected')
      request.send()

      await sleep(requestTimeout + 1)
      expect(request.rejected).toHaveBeenCalled()
      expect(request.entry.reject).toHaveBeenCalledWith(expect.any(KafkaJSRequestTimeoutError))
      expect(timeoutHandler).toHaveBeenCalled()
    })
  })

  describe('#completed', () => {
    it('resolves the promise', () => {
      expect(request.duration).toEqual(null)

      request.send()
      request.completed({ size, payload })

      expect(request.entry.resolve).toHaveBeenCalledWith({
        correlationId: request.correlationId,
        entry: request.entry,
        size,
        payload,
      })

      expect(request.duration).toEqual(expect.any(Number))
    })

    it('does not call resolve more than once', () => {
      request.send()
      request.completed({ size, payload })
      expect(() => request.completed({ size, payload })).toThrow(KafkaJSNonRetriableError)

      expect(request.entry.resolve).toHaveBeenCalledTimes(1)
    })
  })

  describe('#rejected', () => {
    const error = new Error('Test error')

    it('rejects the promise', () => {
      expect(request.duration).toEqual(null)

      request.send()
      request.rejected(error)

      expect(request.entry.reject).toHaveBeenCalledWith(error)
      expect(request.duration).toEqual(expect.any(Number))
    })

    it('does not call reject more than once', () => {
      request.send()
      request.rejected(error)
      expect(() => request.rejected(error)).toThrow(KafkaJSNonRetriableError)

      expect(request.entry.reject).toHaveBeenCalledTimes(1)
    })
  })
})
