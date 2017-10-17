const { secureRandom, connectionOpts, sslConnectionOpts } = require('../../testHelpers')
const { requests } = require('../protocol/requests')
const Connection = require('./connection')

describe('Network > Connection', () => {
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
        expect(connection.connected).toEqual(true)
      })

      test('rejects the Promise in case of errors', async () => {
        connection.host = '99.99.99.99'
        await expect(connection.connect()).rejects.toHaveProperty('message', 'Connection timeout')
        expect(connection.connected).toEqual(false)
      })
    })

    describe('SSL', () => {
      beforeEach(() => {
        connection = new Connection(sslConnectionOpts())
      })

      test('resolves the Promise when connected', async () => {
        await expect(connection.connect()).resolves.toEqual(true)
        expect(connection.connected).toEqual(true)
      })

      test('rejects the Promise in case of timeouts', async () => {
        connection.host = '99.99.99.99'
        await expect(connection.connect()).rejects.toHaveProperty('message', 'Connection timeout')
        expect(connection.connected).toEqual(false)
      })

      test('rejects the Promise in case of errors', async () => {
        connection.ssl.cert = 'invalid'
        const message = 'Failed to connect: error:0906D06C:PEM routines:PEM_read_bio:no start line'
        await expect(connection.connect()).rejects.toHaveProperty('message', message)
        expect(connection.connected).toEqual(false)
      })
    })
  })

  describe('#disconnect', () => {
    beforeEach(() => {
      connection = new Connection(connectionOpts())
    })

    test('disconnects an active connection', async () => {
      await connection.connect()
      expect(connection.connected).toEqual(true)
      await expect(connection.disconnect()).resolves.toEqual(true)
      expect(connection.connected).toEqual(false)
    })
  })

  describe('#send', () => {
    let topicName, metadataProtocol

    beforeEach(() => {
      connection = new Connection(connectionOpts())
      topicName = `test-topic-${secureRandom()}`
      metadataProtocol = requests.Metadata.protocol({ version: 2 })
    })

    test('resolves the Promise with the response', async () => {
      await connection.connect()
      await expect(connection.send(metadataProtocol([topicName]))).resolves.toBeTruthy()
    })

    test('rejects the Promise if it is not connected', async () => {
      expect(connection.connected).toEqual(false)
      await expect(connection.send(metadataProtocol([topicName]))).rejects.toEqual(
        new Error('Not connected')
      )
    })

    test('rejects the Promise in case of a non-retriable error', async () => {
      const protocol = metadataProtocol([topicName])
      protocol.response.parse = () => {
        throw new Error('non-retriable')
      }
      await connection.connect()
      await expect(connection.send(protocol)).rejects.toEqual(new Error('non-retriable'))
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

    test('resets to 0 when correlationId is equal to Number.MAX_VALUE', () => {
      expect(connection.correlationId).toEqual(0)

      connection.nextCorrelationId()
      connection.nextCorrelationId()
      expect(connection.correlationId).toEqual(2)

      connection.correlationId = Number.MAX_VALUE
      const id1 = connection.nextCorrelationId()
      expect(id1).toEqual(0)
      expect(connection.correlationId).toEqual(1)
    })
  })
})
