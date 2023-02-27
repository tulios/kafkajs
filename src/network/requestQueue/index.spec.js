jest.setTimeout(3000)

const sleep = require('../../utils/sleep')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const events = require('../instrumentationEvents')
const RequestQueue = require('./index')
const { KafkaJSInvariantViolation } = require('../../errors')

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

  describe('#waitForPendingRequests', () => {
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
    })

    it('blocks until all pending requests are fulfilled', async () => {
      const emitter = new InstrumentationEventEmitter()
      requestQueue = createRequestQueue({
        instrumentationEmitter: emitter,
      })

      const removeListener = emitter.addListener(events.NETWORK_REQUEST_QUEUE_SIZE, event => {
        if (event.payload.queueSize === 0) {
          requestQueue.fulfillRequest({
            correlationId: request.entry.correlationId,
            payload,
            size,
          })
        }
      })

      requestQueue.maybeThrottle(50)
      requestQueue.push(request)

      expect(requestQueue.pending.length).toEqual(1)
      expect(requestQueue.inflight.size).toEqual(0)

      await requestQueue.waitForPendingRequests()

      removeListener()
      expect(requestQueue.pending.length).toEqual(0)
      expect(requestQueue.inflight.size).toEqual(0)
    })

    it('blocks until the inflight request is timed out', async () => {
      const emitter = new InstrumentationEventEmitter()
      const requestTimeout = 1
      requestQueue = createRequestQueue({
        instrumentationEmitter: emitter,
        enforceRequestTimeout: true,
        requestTimeout: requestTimeout,
      })
      requestQueue.scheduleRequestTimeoutCheck()
      requestQueue.push(request)

      expect(requestQueue.pending.length).toEqual(0)
      expect(requestQueue.inflight.size).toEqual(1)

      await sleep(requestTimeout + 10)

      await requestQueue.waitForPendingRequests()

      expect(requestQueue.pending.length).toEqual(0)
      expect(requestQueue.inflight.size).toEqual(0)
    })
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

    describe('when there are no inflight requests', () => {
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

      describe('when the request does not require a response', () => {
        beforeEach(() => {
          request.expectResponse = false
        })

        it('deletes the inflight request and complete the request when it is processed', () => {
          requestQueue.push(request)

          // Process the queue except the entry for the request, which should get handled automatically
          for (const correlationId of requestQueue.inflight.keys()) {
            if (correlationId !== request.entry.correlationId) {
              requestQueue.fulfillRequest({ correlationId, size: 1, payload: Buffer.from('a') })
            }
          }
          expect(request.entry.resolve).toHaveBeenCalledWith(
            expect.objectContaining({ size: 0, payload: null })
          )

          expect(requestQueue.inflight.size).toEqual(0)
        })
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

    it('respects the client-side throttling', async () => {
      const sendDone = new Promise(resolve => {
        request.sendRequest = () => {
          resolve(Date.now())
        }
      })

      expect(requestQueue.canSendSocketRequestImmediately()).toBe(true)

      const before = Date.now()
      const throttledUntilBefore = requestQueue.throttledUntil
      expect(throttledUntilBefore).toBeLessThan(before)

      const clientSideThrottleTime = 500
      requestQueue.maybeThrottle(clientSideThrottleTime)
      expect(requestQueue.throttledUntil).toBeGreaterThanOrEqual(before + clientSideThrottleTime)
      requestQueue.push(request)

      const sentAt = await sendDone
      expect(sentAt).toBeGreaterThanOrEqual(before + clientSideThrottleTime)
    })

    it('ensure request is sent when client-side throttling delay is marginal', async () => {
      const sendDone = new Promise(resolve => {
        request.sendRequest = () => {
          resolve(Date.now())
        }
      })

      expect(requestQueue.canSendSocketRequestImmediately()).toBe(true)
      const socketRequest = requestQueue.createSocketRequest(request)
      requestQueue.pending.push(socketRequest)

      const before = Date.now()
      const clientSideThrottleTime = 1
      requestQueue.maybeThrottle(clientSideThrottleTime)
      // Sleep until the marginal delay is passed before calling scheduleCheckPendingRequests()
      await sleep(clientSideThrottleTime)
      requestQueue.scheduleCheckPendingRequests()

      const sentAt = await sendDone
      expect(sentAt).toBeGreaterThanOrEqual(before + clientSideThrottleTime)
    })

    it('does not allow for a inflight correlation ids collision', async () => {
      requestQueue.inflight.set(request.entry.correlationId, 'already existing inflight')
      expect(() => {
        requestQueue.push(request)
      }).toThrowError(new KafkaJSInvariantViolation('Correlation id already exists'))
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
    })

    it('deletes the inflight request and calls completed on the request', () => {
      requestQueue.push(request)
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

      it('calls send on the earliest pending request', () => {
        requestQueue.push(request)
        expect(requestQueue.pending.length).toEqual(1)

        const currentInflightSize = requestQueue.inflight.size

        // Pick one of the inflight requests to fulfill
        const correlationId = requestQueue.inflight.keys().next().value
        requestQueue.fulfillRequest({
          correlationId: correlationId,
          payload,
          size,
        })

        expect(send).toHaveBeenCalled()
        expect(requestQueue.pending.length).toEqual(0)
        expect(requestQueue.inflight.size).toEqual(currentInflightSize)
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

      // Pick one of the inflight requests to fulfill
      const correlationId = requestQueue.inflight.keys().next().value
      requestQueue.fulfillRequest({
        correlationId,
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
