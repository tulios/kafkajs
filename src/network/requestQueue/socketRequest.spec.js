const SocketRequest = require('./socketRequest')
const { KafkaJSRequestTimeoutError, KafkaJSNonRetriableError } = require('../../errors')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const events = require('../instrumentationEvents')

describe('Network > SocketRequest', () => {
  let request, sendRequest, timeoutHandler
  let correlationId = 0
  const requestTimeout = 50
  const size = 32
  const payload = { ok: true }

  const createSocketRequest = (args = {}) =>
    new SocketRequest({
      requestTimeout,
      broker: 'localhost:9092',
      clientId: 'KafkaJS',
      expectResponse: true,
      entry: {
        apiKey: 0,
        apiVersion: 4,
        apiName: 'Produce',
        correlationId: correlationId++,
        resolve: jest.fn(),
        reject: jest.fn(),
      },
      ...args,
    })

  beforeEach(() => {
    sendRequest = jest.fn()
    timeoutHandler = jest.fn()
    request = createSocketRequest({
      send: sendRequest,
      timeout: timeoutHandler,
    })
  })

  describe('#send', () => {
    it('sends the request using the provided function', () => {
      expect(request.sentAt).toEqual(null)
      expect(request.pendingDuration).toEqual(null)

      request.enforceRequestTimeout = true
      request.send()

      expect(sendRequest).toHaveBeenCalled()
      expect(request.sentAt).toEqual(expect.any(Number))
      expect(request.pendingDuration).toEqual(expect.any(Number))
    })

    it('does not call sendRequest more than once', () => {
      request.send()
      expect(() => request.send()).toThrow(KafkaJSNonRetriableError)

      expect(sendRequest).toHaveBeenCalledTimes(1)
    })

    it('executes the timeoutHandler when it times out', async () => {
      jest.spyOn(request, 'rejected')
      request.enforceRequestTimeout = true
      request.send()
      request.timeoutRequest()

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

  describe('instrumentation events', () => {
    let emitter, removeListener, eventCalled

    beforeEach(() => {
      eventCalled = jest.fn()
      emitter = new InstrumentationEventEmitter()
      request = request = createSocketRequest({
        send: sendRequest,
        timeout: timeoutHandler,
        instrumentationEmitter: emitter,
      })
    })

    afterEach(() => {
      removeListener && removeListener()
    })

    it('emits NETWORK_REQUEST', () => {
      emitter.addListener(events.NETWORK_REQUEST, eventCalled)
      request.send()
      request.completed({ size, payload })
      expect(eventCalled).toHaveBeenCalledWith({
        id: expect.any(Number),
        type: 'network.request',
        timestamp: expect.any(Number),
        payload: {
          apiKey: 0,
          apiName: 'Produce',
          apiVersion: 4,
          broker: 'localhost:9092',
          clientId: 'KafkaJS',
          correlationId: expect.any(Number),
          createdAt: expect.any(Number),
          duration: expect.any(Number),
          pendingDuration: expect.any(Number),
          sentAt: expect.any(Number),
          size,
        },
      })
    })
  })
})
