const sleep = require('../../utils/sleep')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const events = require('../instrumentationEvents')
const RequestQueue = require('./index')

describe('Network > RequestQueue', () => {
  let requestQueue
  let correlationId = 0

  const createEntry = () => ({
    correlationId: correlationId++,
    resolve: jest.fn(),
    reject: jest.fn(),
  })

  const createRequestQueue = (args = {}) =>
    new RequestQueue({
      maxInFlightRequests: 2,
      requestTimeout: 50,
      clientId: 'KafkaJS',
      broker: 'localhost:9092',
      logger: newLogger(),
      ...args,
    })

  beforeEach(() => {
    requestQueue = createRequestQueue()
  })

  describe('#push', () => {
    let request, send

    beforeEach(() => {
      send = jest.fn()
      request = {
        sendRequest: send,
        entry: createEntry(),
        expectResponse: true,
      }
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
        requestQueue.push(request)
        expect(request.entry.resolve).toHaveBeenCalledWith(
          expect.objectContaining({ size: 0, payload: null })
        )

        expect(requestQueue.inflight.size).toEqual(0)
      })
    })

    describe('when there are many inflight requests', () => {
      beforeEach(() => {
        while (requestQueue.inflight.size < requestQueue.maxInFlightRequests) {
          const request = {
            sendRequest: jest.fn(),
            entry: createEntry(),
            expectResponse: true,
          }

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

  describe('#fulfillRequest', () => {
    let request, send, size, payload

    beforeEach(() => {
      send = jest.fn()
      payload = { ok: true }
      size = 32
      request = {
        sendRequest: send,
        entry: createEntry(),
        expectResponse: true,
      }

      requestQueue.push(request)
    })

    it('deletes the inflight request and calls completed on the request', () => {
      expect(requestQueue.inflight.size).toEqual(1)

      requestQueue.fulfillRequest({
        correlationId: request.entry.correlationId,
        payload,
        size,
      })

      expect(requestQueue.inflight.size).toEqual(0)
      expect(request.entry.resolve).toHaveBeenCalledWith(expect.objectContaining({ size, payload }))
    })

    describe('when there are pending requests', () => {
      beforeEach(() => {
        while (requestQueue.inflight.size < requestQueue.maxInFlightRequests) {
          const request = {
            sendRequest: jest.fn(),
            entry: createEntry(),
            expectResponse: true,
          }

          requestQueue.push(request)
        }
      })

      it('calls send on the latest pending request', () => {
        requestQueue.push(request)
        expect(requestQueue.pending.length).toEqual(1)

        const currentInflightSize = requestQueue.inflight.size

        requestQueue.fulfillRequest({
          correlationId: request.entry.correlationId,
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
        const request = {
          sendRequest: jest.fn(),
          entry: createEntry(),
          expectResponse: true,
        }

        requestQueue.push(request)
        allRequests.push(request)
      }

      const pendingRequest = {
        sendRequest: jest.fn(),
        entry: createEntry(),
        expectResponse: true,
      }

      requestQueue.push(pendingRequest)
      allRequests.push(pendingRequest)

      expect(requestQueue.inflight.size).toEqual(requestQueue.maxInFlightRequests)
      expect(requestQueue.pending.length).toEqual(1)

      const error = new Error('Broker closed the connection')
      requestQueue.rejectAll(error)

      expect(requestQueue.inflight.size).toEqual(0)
      expect(requestQueue.pending.length).toEqual(0)

      for (const request of allRequests) {
        expect(request.entry.reject).toHaveBeenCalledWith(error)
      }
    })
  })

  describe('instrumentation events', () => {
    let emitter, removeListener, eventCalled, request, payload, size, requestTimeout

    beforeEach(() => {
      requestTimeout = 1
      eventCalled = jest.fn()
      emitter = new InstrumentationEventEmitter()
      requestQueue = createRequestQueue({
        instrumentationEmitter: emitter,
        enforceRequestTimeout: true,
        requestTimeout,
      })
      request = {
        sendRequest: jest.fn(),
        entry: createEntry(),
        expectResponse: true,
      }
      payload = { ok: true }
      size = 32
    })

    afterEach(() => {
      removeListener && removeListener()
    })

    it('does not emit the event if the queue size remains the same', () => {
      emitter.addListener(events.NETWORK_REQUEST_QUEUE_SIZE, eventCalled)
      expect(requestQueue.pending.length).toEqual(0)
      requestQueue.push(request)
      expect(requestQueue.pending.length).toEqual(0)
      expect(eventCalled).not.toHaveBeenCalled()
    })

    it('emits NETWORK_REQUEST_QUEUE_SIZE when a new request is added', () => {
      emitter.addListener(events.NETWORK_REQUEST_QUEUE_SIZE, eventCalled)

      while (requestQueue.inflight.size < requestQueue.maxInFlightRequests) {
        const request = {
          sendRequest: jest.fn(),
          entry: createEntry(),
          expectResponse: true,
        }

        requestQueue.push(request)
      }

      requestQueue.push(request)
      expect(eventCalled).toHaveBeenCalledWith({
        id: expect.any(Number),
        type: 'network.request_queue_size',
        timestamp: expect.any(Number),
        payload: {
          broker: 'localhost:9092',
          clientId: 'KafkaJS',
          queueSize: requestQueue.pending.length,
        },
      })
    })

    it('emits NETWORK_REQUEST_QUEUE_SIZE when a request is removed', () => {
      emitter.addListener(events.NETWORK_REQUEST_QUEUE_SIZE, eventCalled)

      while (requestQueue.inflight.size < requestQueue.maxInFlightRequests) {
        const request = {
          sendRequest: jest.fn(),
          entry: createEntry(),
          expectResponse: true,
        }

        requestQueue.push(request)
      }

      requestQueue.push(request)
      requestQueue.fulfillRequest({
        correlationId: request.entry.correlationId,
        payload,
        size,
      })

      expect(eventCalled).toHaveBeenCalledTimes(2)
      expect(eventCalled).toHaveBeenCalledWith({
        id: expect.any(Number),
        type: 'network.request_queue_size',
        timestamp: expect.any(Number),
        payload: {
          broker: 'localhost:9092',
          clientId: 'KafkaJS',
          queueSize: 0,
        },
      })
    })

    it('emits NETWORK_REQUEST_QUEUE_SIZE when the requests are rejected', () => {
      emitter.addListener(events.NETWORK_REQUEST_QUEUE_SIZE, eventCalled)
      const error = new Error('Broker closed the connection')
      requestQueue.rejectAll(error)

      expect(eventCalled).toHaveBeenCalledWith({
        id: expect.any(Number),
        type: 'network.request_queue_size',
        timestamp: expect.any(Number),
        payload: {
          broker: 'localhost:9092',
          clientId: 'KafkaJS',
          queueSize: 0,
        },
      })
    })

    it('emits NETWORK_REQUEST_TIMEOUT', async () => {
      emitter.addListener(events.NETWORK_REQUEST_TIMEOUT, eventCalled)
      requestQueue.scheduleRequestTimeoutCheck()
      requestQueue.push(request)

      await sleep(requestTimeout + 10)

      expect(eventCalled).toHaveBeenCalledWith({
        id: expect.any(Number),
        type: 'network.request_timeout',
        timestamp: expect.any(Number),
        payload: {
          apiKey: request.entry.apiKey,
          apiName: request.entry.apiName,
          apiVersion: request.entry.apiVersion,
          broker: 'localhost:9092',
          clientId: 'KafkaJS',
          correlationId: expect.any(Number),
          createdAt: expect.any(Number),
          pendingDuration: expect.any(Number),
          sentAt: expect.any(Number),
        },
      })
    })
  })
})
