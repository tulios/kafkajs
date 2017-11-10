const { connectionOpts, sslConnectionOpts } = require('../../testHelpers')
const { requests } = require('../protocol/requests')
const Connection = require('./connection')

describe('Network > Connection', () => {
  // According to RFC 5737:
  // The blocks 192.0.2.0/24 (TEST-NET-1), 198.51.100.0/24 (TEST-NET-2),
  // and 203.0.113.0/24 (TEST-NET-3) are provided for use in documentation.
  const invalidIP = '203.0.113.1'
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
        connection.host = invalidIP
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
        connection.host = invalidIP
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
    let apiVersions

    beforeEach(() => {
      connection = new Connection(connectionOpts())
      apiVersions = requests.ApiVersions.protocol({ version: 0 })
    })

    test('resolves the Promise with the response', async () => {
      await connection.connect()
      await expect(connection.send(apiVersions())).resolves.toBeTruthy()
    })

    test('rejects the Promise if it is not connected', async () => {
      expect(connection.connected).toEqual(false)
      await expect(connection.send(apiVersions())).rejects.toEqual(new Error('Not connected'))
    })

    test('rejects the Promise in case of a non-retriable error', async () => {
      const protocol = apiVersions()
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
