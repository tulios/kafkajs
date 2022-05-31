const { connectionOpts, sslConnectionOpts } = require('../../testHelpers')
const sleep = require('../utils/sleep')
const { requests } = require('../protocol/requests')
const Decoder = require('../protocol/decoder')
const Encoder = require('../protocol/encoder')
const { KafkaJSRequestTimeoutError } = require('../errors')
const Connection = require('./connection')
const { CONNECTION_STATUS } = require('./connectionStatus')
const EventEmitter = require('events')

describe('Network > Connection', () => {
  const invalidHost = 'kafkajs.test'
  let connection

  afterEach(async () => {
    connection && (await connection.disconnect())
  })

  describe('#connect', () => {
    describe('PLAINTEXT', () => {
      beforeEach(() => {
        connection = new Connection(connectionOpts())
      })

      test('resolves the Promise when connected', async () => {
        await expect(connection.connect()).resolves.toEqual(true)
        expect(connection.isConnected()).toEqual(true)
      })

      test('rejects the Promise in case of errors', async () => {
        connection.host = invalidHost
        const messagePattern = /Connection error: getaddrinfo ENOTFOUND kafkajs.test/
        await expect(connection.connect()).rejects.toThrow(messagePattern)
        expect(connection.isConnected()).toEqual(false)
      })
    })

    describe('SSL', () => {
      beforeEach(() => {
        connection = new Connection(sslConnectionOpts())
      })

      test('resolves the Promise when connected', async () => {
        await expect(connection.connect()).resolves.toEqual(true)
        expect(connection.isConnected()).toEqual(true)
      })

      test('rejects the Promise in case of timeouts', async () => {
        const socketFactory = () => {
          const socket = new EventEmitter()
          socket.end = () => {}
          socket.unref = () => {}
          return socket
        }
        connection = new Connection({
          ...sslConnectionOpts({ connectionTimeout: 1 }),
          socketFactory,
        })

        await expect(connection.connect()).rejects.toHaveProperty('message', 'Connection timeout')
        expect(connection.isConnected()).toEqual(false)
      })

      test('rejects the Promise in case of errors', async () => {
        connection.ssl.cert = 'invalid'
        const messagePattern = /Failed to connect/
        await expect(connection.connect()).rejects.toThrow(messagePattern)
        expect(connection.isConnected()).toEqual(false)
      })

      test('sets the authenticatedAt timer', async () => {
        connection.authenticatedAt = process.hrtime()
        await connection.connect()
        expect(connection.authenticatedAt).toBe(null)
      })
    })
  })

  describe('#disconnect', () => {
    beforeEach(() => {
      connection = new Connection(connectionOpts())
    })

    test('disconnects an active connection', async () => {
      await connection.connect()
      expect(connection.isConnected()).toEqual(true)
      await expect(connection.disconnect()).resolves.toEqual(true)
      expect(connection.isConnected()).toEqual(false)
    })

    test('trigger "end" and "unref" function on not active connection', async () => {
      expect(connection.isConnected()).toEqual(false)
      connection.socket = {
        end: jest.fn(),
        unref: jest.fn(),
      }
      await expect(connection.disconnect()).resolves.toEqual(true)
      expect(connection.socket.end).toHaveBeenCalled()
      expect(connection.socket.unref).toHaveBeenCalled()
    })
  })

  describe('#send', () => {
    let apiVersions, metadata

    beforeEach(() => {
      connection = new Connection(connectionOpts())
      apiVersions = requests.ApiVersions.protocol({ version: 0 })
      metadata = requests.Metadata.protocol({ version: 0 })
    })

    test('resolves the Promise with the response', async () => {
      await connection.connect()
      await expect(connection.send(apiVersions())).resolves.toBeTruthy()
    })

    test('rejects the Promise if it is not connected', async () => {
      expect(connection.isConnected()).toEqual(false)
      await expect(connection.send(apiVersions())).rejects.toEqual(new Error('Not connected'))
    })

    test('rejects the Promise in case of a non-retriable error', async () => {
      const protocol = {
        ...apiVersions(),
        response: {
          ...apiVersions().response,
          parse: () => {
            throw new Error('non-retriable')
          },
        },
      }

      await connection.connect()
      await expect(connection.send(protocol)).rejects.toEqual(new Error('non-retriable'))
    })

    test('respect the maxInFlightRequests', async () => {
      const protocol = apiVersions()
      connection = new Connection(connectionOpts({ maxInFlightRequests: 2 }))
      const originalProcessData = connection.processData

      connection.processData = async data => {
        await sleep(100)
        originalProcessData.apply(connection, [data])
      }

      await connection.connect()

      const requests = [
        connection.send(protocol),
        connection.send(protocol),
        connection.send(protocol),
      ]

      await sleep(50)

      const inFlightRequestsSize = connection.requestQueue.inflight.size
      const pendingRequestsSize = connection.requestQueue.pending.length

      await Promise.all(requests)

      expect(inFlightRequestsSize).toEqual(2)
      expect(pendingRequestsSize).toEqual(1)
    })

    test('respect the requestTimeout', async () => {
      const protocol = apiVersions()
      connection = new Connection(
        connectionOpts({
          requestTimeout: 50,
          enforceRequestTimeout: true,
        })
      )
      const originalProcessData = connection.processData

      connection.processData = async data => {
        await sleep(100)
        originalProcessData.apply(connection, [data])
      }

      await connection.connect()
      await expect(connection.send(protocol)).rejects.toThrowError(KafkaJSRequestTimeoutError)
    })

    test('throttles the request queue', async () => {
      const clientSideThrottleTime = 500
      // Create a fictitious request with a response that indicates client-side throttling is needed
      const protocol = {
        request: {
          apiKey: -1,
          apiVersion: 0,
          expectResponse: () => true,
          encode: () => new Encoder(),
        },
        response: {
          decode: () => ({ clientSideThrottleTime }),
          parse: () => ({}),
        },
      }

      // Setup the socket connection to accept the request
      const correlationId = 383
      connection.nextCorrelationId = () => correlationId
      connection.connectionStatus = CONNECTION_STATUS.CONNECTED
      connection.socket = {
        write() {
          // Simulate a happy response
          setImmediate(() => {
            connection.requestQueue.fulfillRequest({ correlationId, size: 0, payload: null })
          })
        },
        end() {},
        unref() {},
      }
      const before = Date.now()
      await connection.send(protocol)
      expect(connection.requestQueue.throttledUntil).toBeGreaterThanOrEqual(
        before + clientSideThrottleTime
      )
    })

    describe('Debug logging', () => {
      let initialValue, connection

      beforeAll(() => {
        initialValue = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
      })

      afterAll(() => {
        process.env['KAFKAJS_DEBUG_PROTOCOL_BUFFERS'] = initialValue
      })

      afterEach(async () => {
        connection && (await connection.disconnect())
      })

      test('logs the full payload in case of non-retriable error when "KAFKAJS_DEBUG_PROTOCOL_BUFFERS" runtime flag is set', async () => {
        process.env['KAFKAJS_DEBUG_PROTOCOL_BUFFERS'] = '1'
        connection = new Connection(connectionOpts())
        const debugStub = jest.fn()
        connection.logger.debug = debugStub
        const protocol = apiVersions()
        protocol.response.parse = () => {
          throw new Error('non-retriable')
        }
        await connection.connect()
        await expect(connection.send(protocol)).rejects.toBeTruthy()

        const lastCall = debugStub.mock.calls[debugStub.mock.calls.length - 1]
        expect(lastCall[1].payload).toEqual(expect.any(Buffer))
      })

      test('filters payload in case of non-retriable error when "KAFKAJS_DEBUG_PROTOCOL_BUFFERS" runtime flag is not set', async () => {
        delete process.env['KAFKAJS_DEBUG_PROTOCOL_BUFFERS']
        connection = new Connection(connectionOpts())
        const debugStub = jest.fn()
        connection.logger.debug = debugStub
        const protocol = apiVersions()
        protocol.response.parse = () => {
          throw new Error('non-retriable')
        }
        await connection.connect()
        await expect(connection.send(protocol)).rejects.toBeTruthy()

        const lastCall = debugStub.mock.calls[debugStub.mock.calls.length - 1]
        expect(lastCall[1].payload).toEqual({
          type: 'Buffer',
          data: '[filtered]',
        })
      })
    })

    describe('Error logging', () => {
      let connection, errorStub

      beforeEach(() => {
        connection = new Connection(connectionOpts())
        errorStub = jest.fn()
        connection.logger.error = errorStub
      })

      afterEach(async () => {
        connection && (await connection.disconnect())
      })

      it('logs error responses by default', async () => {
        const protocol = metadata({ topics: [] })
        protocol.response.parse = () => {
          throw new Error('non-retriable')
        }

        expect(protocol.logResponseError).not.toBe(false)

        await connection.connect()

        await expect(connection.send(protocol)).rejects.toBeTruthy()

        expect(errorStub).toHaveBeenCalled()
      })

      it('does not log errors when protocol.logResponseError=false', async () => {
        const protocol = metadata({ topics: [] })
        protocol.response.parse = () => {
          throw new Error('non-retriable')
        }
        protocol.logResponseError = false
        await connection.connect()

        await expect(connection.send(protocol)).rejects.toBeTruthy()

        expect(errorStub).not.toHaveBeenCalled()
      })
    })
  })

  describe('#nextCorrelationId', () => {
    beforeEach(() => {
      connection = new Connection(connectionOpts())
    })

    test('increments the current correlationId', () => {
      const id1 = connection.nextCorrelationId()
      const id2 = connection.nextCorrelationId()
      expect(id1).toEqual(0)
      expect(id2).toEqual(1)
      expect(connection.correlationId).toEqual(2)
    })

    test('resets to 0 when correlationId is equal to max signed int32', () => {
      expect(connection.correlationId).toEqual(0)

      connection.nextCorrelationId()
      connection.nextCorrelationId()
      expect(connection.correlationId).toEqual(2)

      connection.correlationId = Math.pow(2, 31) - 1
      const id1 = connection.nextCorrelationId()
      expect(id1).toEqual(0)
      expect(connection.correlationId).toEqual(1)
    })
  })

  describe('#processData', () => {
    beforeEach(() => {
      connection = new Connection(connectionOpts())
    })

    test('buffer data while it is not complete', () => {
      const correlationId = 1
      const resolve = jest.fn()
      const entry = { correlationId, resolve }

      connection.requestQueue.push({
        entry,
        expectResponse: true,
        sendRequest: jest.fn(),
      })

      const payload = Buffer.from('ab')
      const size = Buffer.byteLength(payload) + Decoder.int32Size()
      // expected response size
      const sizePart1 = Buffer.from([0, 0])
      const sizePart2 = Buffer.from([0, 6])

      const correlationIdPart1 = Buffer.from([0, 0])
      const correlationIdPart2 = Buffer.from([0])
      const correlationIdPart3 = Buffer.from([1])

      // write half of the expected size and expect to keep buffering
      expect(connection.processData(sizePart1)).toBeUndefined()
      expect(resolve).not.toHaveBeenCalled()

      // Write the rest of the size, but without any response
      expect(connection.processData(sizePart2)).toBeUndefined()
      expect(resolve).not.toHaveBeenCalled()

      // Response consists of correlation id + payload
      // Writing 1/3 of the correlation id
      expect(connection.processData(correlationIdPart1)).toBeUndefined()
      expect(resolve).not.toHaveBeenCalled()

      // At this point, we will write N bytes, where N == size,
      // but we should keep buffering because the size field should
      // not be considered as part of the response payload
      expect(connection.processData(correlationIdPart2)).toBeUndefined()
      expect(resolve).not.toHaveBeenCalled()

      // write full payload size
      const buffer = Buffer.concat([correlationIdPart3, payload])
      connection.processData(buffer)

      expect(resolve).toHaveBeenCalledWith({
        correlationId,
        size,
        entry,
        payload,
      })
    })
  })
})
