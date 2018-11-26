const { newLogger } = require('testHelpers')

const RequestQueue = require('./index')
const SocketRequest = require('./socketRequest')

describe('Network > RequestQueue', () => {
  let requestQueue
  let correlationId = 0

  const createEntry = () => ({
    correlationId: correlationId++,
    resolve: jest.fn(),
    reject: jest.fn(),
  })

  beforeEach(() => {
    requestQueue = new RequestQueue({
      maxInFlightRequests: 2,
      requestTimeout: 50,
      clientId: 'KafkaJS',
      broker: 'localhost:9092',
      logger: newLogger(),
    })
  })

  describe('#createRequest', () => {
    it('creates a SocketRequest with broker and requestTimeout', () => {
      const entry = createEntry()
      const send = jest.fn()
      const request = requestQueue.createRequest({
        entry,
        send,
        expectResponse: true,
      })

      expect(request).toBeInstanceOf(SocketRequest)
      expect(request.broker).toEqual(requestQueue.broker)
      expect(request.requestTimeout).toEqual(requestQueue.requestTimeout)
    })
  })

  describe('#push', () => {
    let request, send

    beforeEach(() => {
      send = jest.fn()
      request = requestQueue.createRequest({
        send,
        entry: createEntry(),
        expectResponse: true,
      })
    })

    it('sets the timeoutHandler on the request', () => {
      expect(request.timeoutHandler).toEqual(null)
      requestQueue.push(request)
      expect(request.timeoutHandler).toEqual(expect.any(Function))
    })

    it('calls send on the request', () => {
      requestQueue.push(request)
      expect(send).toHaveBeenCalledTimes(1)
    })

    describe('when the request does not require a response', () => {
      beforeEach(() => {
        request.expectResponse = false
      })

      it('deletes the inflight request and complete the request', () => {
        request.completed = jest.fn()
        requestQueue.push(request)
        expect(request.completed).toHaveBeenCalledWith({ size: 0, payload: null })
        expect(requestQueue.inflight.size).toEqual(0)
      })
    })

    describe('when there are many inflight requests', () => {
      beforeEach(() => {
        while (requestQueue.inflight.size < requestQueue.maxInFlightRequests) {
          const request = requestQueue.createRequest({
            send: jest.fn(),
            entry: createEntry(),
            expectResponse: true,
          })

          requestQueue.push(request)
        }
      })

      it('adds the new request to the pending queue', () => {
        expect(requestQueue.inflight.size).toEqual(requestQueue.maxInFlightRequests)
        requestQueue.push(request)
        expect(requestQueue.inflight.size).toEqual(requestQueue.maxInFlightRequests)
        expect(requestQueue.pending.length).toEqual(1)
      })

      describe('when maxInFlightRequests is null', () => {
        let maxInFlightRequests

        beforeEach(() => {
          maxInFlightRequests = requestQueue.maxInFlightRequests
          requestQueue.maxInFlightRequests = null
        })

        it('does not enforce the number of inflight requests', () => {
          expect(requestQueue.inflight.size).toEqual(maxInFlightRequests)
          requestQueue.push(request)
          expect(requestQueue.inflight.size).toEqual(maxInFlightRequests + 1)
        })
      })
    })
  })

  describe('#onResponse', () => {
    let request, send, size, payload

    beforeEach(() => {
      send = jest.fn()
      payload = { ok: true }
      size = 32
      request = requestQueue.createRequest({
        send,
        entry: createEntry(),
        expectResponse: true,
      })

      requestQueue.push(request)
    })

    it('deletes the inflight request and calls completed on the request', () => {
      request.completed = jest.fn()
      expect(requestQueue.inflight.size).toEqual(1)

      requestQueue.onResponse({
        correlationId: request.correlationId,
        payload,
        size,
      })

      expect(requestQueue.inflight.size).toEqual(0)
      expect(request.completed).toHaveBeenCalledWith({ size, payload })
    })

    describe('when there are pending requests', () => {
      beforeEach(() => {
        while (requestQueue.inflight.size < requestQueue.maxInFlightRequests) {
          const request = requestQueue.createRequest({
            send: jest.fn(),
            entry: createEntry(),
            expectResponse: true,
          })

          requestQueue.push(request)
        }
      })

      it('calls send on the latest pending request', () => {
        requestQueue.push(request)
        expect(requestQueue.pending.length).toEqual(1)

        const currentInflightSize = requestQueue.inflight.size
        const inflightRequest = Array.from(requestQueue.inflight.values()).pop()
        inflightRequest.completed = jest.fn()

        requestQueue.onResponse({
          correlationId: inflightRequest.correlationId,
          payload,
          size,
        })

        expect(send).toHaveBeenCalled()
        expect(requestQueue.pending.length).toEqual(0)
        expect(requestQueue.inflight.size).toEqual(currentInflightSize - 1)
      })
    })
  })

  describe('#rejectAll', () => {
    it('calls rejected on all requests (inflight + pending)', () => {
      const allRequests = []
      while (requestQueue.inflight.size < requestQueue.maxInFlightRequests) {
        const request = requestQueue.createRequest({
          send: jest.fn(),
          entry: createEntry(),
          expectResponse: true,
        })

        requestQueue.push(request)
        allRequests.push(request)
      }

      const pendingRequest = requestQueue.createRequest({
        send: jest.fn(),
        entry: createEntry(),
        expectResponse: true,
      })

      requestQueue.push(pendingRequest)
      allRequests.push(pendingRequest)

      expect(requestQueue.inflight.size).toEqual(requestQueue.maxInFlightRequests)
      expect(requestQueue.pending.length).toEqual(1)

      for (let request of allRequests) {
        request.rejected = jest.fn()
      }

      const error = new Error('Broker closed the connection')
      requestQueue.rejectAll(error)

      expect(requestQueue.inflight.size).toEqual(0)
      expect(requestQueue.pending.length).toEqual(0)

      for (let request of allRequests) {
        expect(request.rejected).toHaveBeenCalledWith(error)
      }
    })
  })
})
