const {
  createConnectionBuilder,
  createConnection,
  newLogger,
  secureRandom,
} = require('testHelpers')
const { KafkaJSProtocolError, KafkaJSConnectionError } = require('../errors')
const BrokerPool = require('./brokerPool')
const Broker = require('../broker')

describe('Cluster > BrokerPool', () => {
  let topicName, brokerPool

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    brokerPool = new BrokerPool({
      connectionBuilder: createConnectionBuilder(),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    await brokerPool.disconnect()
  })

  describe('#connect', () => {
    it('connects to the seed broker when the broker pool is created', async () => {
      expect(brokerPool.seedBroker.isConnected()).toEqual(false)
      await brokerPool.connect()
      expect(brokerPool.seedBroker.isConnected()).toEqual(true)
    })

    test('load the versions from the seed broker', async () => {
      expect(brokerPool.versions).toEqual(null)
      await brokerPool.connect()
      expect(brokerPool.versions).toEqual(brokerPool.seedBroker.versions)
    })

    test('select a different seed broker on ILLEGAL_SASL_STATE error', async () => {
      const originalSeedPort = brokerPool.seedBroker.connection.port
      const illegalStateError = new KafkaJSProtocolError({
        message: 'ILLEGAL_SASL_STATE',
        type: 'ILLEGAL_SASL_STATE',
        code: 34,
      })

      brokerPool.seedBroker.connect = jest.fn(() => {
        throw illegalStateError
      })

      await brokerPool.connect()
      expect(brokerPool.seedBroker.connection.port).not.toEqual(originalSeedPort)
    })

    test('select a different seed broker on connection errors', async () => {
      const originalSeedPort = brokerPool.seedBroker.connection.port
      brokerPool.seedBroker.connect = jest.fn(() => {
        throw new KafkaJSConnectionError('Test connection error')
      })

      await brokerPool.connect()
      expect(brokerPool.seedBroker.connection.port).not.toEqual(originalSeedPort)
    })

    it('does not connect to the seed broker if it is already connected', async () => {
      await brokerPool.connect()
      expect(brokerPool.seedBroker.isConnected()).toEqual(true)

      jest.spyOn(brokerPool.seedBroker, 'connect')
      await brokerPool.connect()
      expect(brokerPool.seedBroker.connect).not.toHaveBeenCalled()
    })

    it('does not connect to the seed broker if it has connected brokers', async () => {
      await brokerPool.connect()
      await brokerPool.refreshMetadata([topicName])

      const broker = Object.values(brokerPool.brokers).find(broker => !broker.isConnected())
      expect(broker).not.toEqual(brokerPool.seedBroker)

      await broker.connect()
      expect(broker.isConnected()).toEqual(true)

      await brokerPool.seedBroker.disconnect()
      expect(brokerPool.seedBroker.isConnected()).toEqual(false)

      jest.spyOn(brokerPool.seedBroker, 'connect')
      await brokerPool.connect()
      expect(brokerPool.seedBroker.connect).not.toHaveBeenCalled()
    })
  })

  describe('#disconnect', () => {
    beforeEach(async () => {
      await brokerPool.connect()
    })

    it('disconnects the seed broker', async () => {
      expect(brokerPool.seedBroker.isConnected()).toEqual(true)
      await brokerPool.disconnect()
      expect(brokerPool.seedBroker.isConnected()).toEqual(false)
    })

    it('disconnects all brokers in the broker pool', async () => {
      await brokerPool.refreshMetadata([topicName])
      const brokers = Object.values(brokerPool.brokers)

      for (let broker of brokers) {
        await brokerPool.connectBroker(broker)
      }

      expect(brokerPool.hasConnectedBrokers()).toEqual(true)
      await brokerPool.disconnect()

      for (let broker of brokers) {
        expect(broker.isConnected()).toEqual(false)
      }
    })

    it('erases metadata and broker pool information', async () => {
      await brokerPool.refreshMetadata([topicName])
      expect(brokerPool.metadata).not.toEqual(null)
      expect(brokerPool.versions).not.toEqual(null)
      expect(brokerPool.brokers).not.toEqual({})

      await brokerPool.disconnect()
      expect(brokerPool.metadata).toEqual(null)
      expect(brokerPool.versions).toEqual(null)
      expect(brokerPool.brokers).toEqual({})
    })
  })

  describe('#refreshMetadata', () => {
    beforeEach(async () => {
      await brokerPool.connect()
    })

    it('updates the metadata object', async () => {
      expect(brokerPool.metadata).toEqual(null)
      await brokerPool.refreshMetadata([topicName])
      expect(brokerPool.metadata).not.toEqual(null)
    })

    it('updates the list of brokers', async () => {
      expect(brokerPool.brokers).toEqual({})
      await brokerPool.refreshMetadata([topicName])
      expect(Object.keys(brokerPool.brokers).sort()).toEqual(['0', '1', '2'])
      expect(Object.values(brokerPool.brokers)).toEqual(
        expect.arrayContaining([expect.any(Broker), expect.any(Broker), expect.any(Broker)])
      )
    })

    it('includes the seed broker into the broker pool', async () => {
      await brokerPool.refreshMetadata([topicName])
      const seed = brokerPool.seedBroker.connection
      const brokers = Object.values(brokerPool.brokers)
      const seedFromBrokerPool = brokers
        .map(b => b.connection)
        .find(b => b.host === seed.host && b.port === seed.port)

      expect(seedFromBrokerPool).toEqual(seed)
    })

    it('retries on LEADER_NOT_AVAILABLE errors', async () => {
      const leaderNotAvailableError = new KafkaJSProtocolError({
        message: 'LEADER_NOT_AVAILABLE',
        type: 'LEADER_NOT_AVAILABLE',
        code: 5,
      })

      brokerPool.findConnectedBroker = jest.fn(() => brokerPool.seedBroker)
      jest.spyOn(brokerPool.seedBroker, 'metadata').mockImplementationOnce(() => {
        throw leaderNotAvailableError
      })

      expect(brokerPool.metadata).toEqual(null)
      await brokerPool.refreshMetadata([topicName])
      expect(brokerPool.metadata).not.toEqual(null)
    })
  })

  describe('#findBroker', () => {
    beforeEach(async () => {
      await brokerPool.refreshMetadata([topicName])
    })

    it('finds a broker by nodeId', async () => {
      const nodeId = Object.keys(brokerPool.brokers)[0]
      const broker = await brokerPool.findBroker({ nodeId })
      expect(broker).toEqual(brokerPool.brokers[nodeId])
    })

    it('connects the broker if it is not connected', async () => {
      const nodeId = Object.keys(brokerPool.brokers).find(
        id => !brokerPool.brokers[id].isConnected()
      )
      expect(brokerPool.brokers[nodeId].isConnected()).toEqual(false)

      const broker = await brokerPool.findBroker({ nodeId })
      expect(broker.isConnected()).toEqual(true)
    })

    it('recreates the connection on connection errors', async () => {
      const nodeId = 'fakebroker'
      const mockBroker = new Broker(createConnection(), newLogger())
      jest.spyOn(mockBroker, 'connect').mockImplementationOnce(() => {
        throw new KafkaJSConnectionError('Connection lost')
      })
      brokerPool.brokers[nodeId] = mockBroker

      const broker = await brokerPool.findBroker({ nodeId })
      expect(broker.isConnected()).toEqual(true)
    })

    it('throws an error when the broker is not found', async () => {
      await expect(brokerPool.findBroker({ nodeId: 627 })).rejects.toHaveProperty(
        'message',
        'Broker 627 not found in the cached metadata'
      )
    })
  })

  describe('#withBroker', () => {
    beforeEach(async () => {
      await brokerPool.refreshMetadata([topicName])
    })

    it('throws an error if there are no brokers configured', async () => {
      brokerPool.brokers = {}
      await expect(brokerPool.withBroker(jest.fn())).rejects.toHaveProperty(
        'message',
        'No brokers in the broker pool'
      )
    })

    it('returns the result of the callback', async () => {
      const output = 'return-from-callback'
      const callback = jest.fn(() => output)
      await expect(brokerPool.withBroker(callback)).resolves.toEqual(output)
      expect(callback).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeId: expect.any(String),
          broker: expect.any(Broker),
        })
      )
    })

    it('returns null if the callback never resolves', async () => {
      const callback = jest.fn(() => {
        throw new Error('never again!')
      })

      await expect(brokerPool.withBroker(callback)).resolves.toEqual(null)
    })
  })

  describe('#findConnectedBroker', () => {
    beforeEach(async () => {
      await brokerPool.refreshMetadata([topicName])
    })

    it('returns a connected broker if it is available', async () => {
      const broker = await brokerPool.findConnectedBroker()
      expect(broker).toBeInstanceOf(Broker)
      expect(broker.isConnected()).toEqual(true)
    })

    it('returns a known broker connecting it in the process', async () => {
      for (let broker of Object.values(brokerPool.brokers)) {
        await broker.disconnect()
      }

      const broker = await brokerPool.findConnectedBroker()
      expect(broker).toBeInstanceOf(Broker)
      expect(broker.isConnected()).toEqual(true)
    })

    it('returns the seed broker if no other broker is available', async () => {
      brokerPool.brokers = {}
      const broker = await brokerPool.findConnectedBroker()
      expect(broker).toEqual(brokerPool.seedBroker)
    })

    it('connects the seed broker if needed', async () => {
      brokerPool.brokers = {}
      await brokerPool.seedBroker.disconnect()

      const broker = await brokerPool.findConnectedBroker()
      expect(broker).toEqual(brokerPool.seedBroker)
      expect(broker.isConnected()).toEqual(true)
    })
  })
})
