const {
  createConnectionBuilder,
  plainTextBrokers,
  createConnectionPool,
  newLogger,
  secureRandom,
} = require('testHelpers')
const { KafkaJSProtocolError, KafkaJSConnectionError } = require('../../errors')
const { createErrorFromCode, errorCodes } = require('../../protocol/error')
const BrokerPool = require('../brokerPool')
const Broker = require('../../broker')

describe('Cluster > BrokerPool', () => {
  let topicName, brokerPool

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    brokerPool = new BrokerPool({
      connectionPoolBuilder: createConnectionBuilder(),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    brokerPool && (await brokerPool.disconnect())
  })

  it('defaults metadataMaxAge to 0', () => {
    expect(brokerPool.metadataMaxAge).toEqual(0)
  })

  describe('#connect', () => {
    it('when the broker pool is created seed broker is null', async () => {
      expect(brokerPool.seedBroker).toEqual(undefined)
      await brokerPool.connect()
      expect(brokerPool.seedBroker.isConnected()).toEqual(true)
    })

    test('load the versions from the seed broker', async () => {
      expect(brokerPool.versions).toEqual(null)
      await brokerPool.connect()
      expect(brokerPool.versions).toEqual(brokerPool.seedBroker.versions)
    })

    test('select a different seed broker on ILLEGAL_SASL_STATE error', async () => {
      await brokerPool.createSeedBroker()

      const originalSeedPort = brokerPool.seedBroker.connectionPool.port
      const illegalStateError = new KafkaJSProtocolError({
        message: 'ILLEGAL_SASL_STATE',
        type: 'ILLEGAL_SASL_STATE',
        code: 34,
      })

      brokerPool.seedBroker.connect = jest.fn(() => {
        throw illegalStateError
      })

      await brokerPool.connect()
      expect(brokerPool.seedBroker.connectionPool.port).not.toEqual(originalSeedPort)
    })

    test('select a different seed broker on connection errors', async () => {
      await brokerPool.createSeedBroker()

      const originalSeedPort = brokerPool.seedBroker.connectionPool.port
      brokerPool.seedBroker.connect = jest.fn(() => {
        throw new KafkaJSConnectionError('Test connection error')
      })

      await brokerPool.connect()
      expect(brokerPool.seedBroker.connectionPool.port).not.toEqual(originalSeedPort)
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

      for (const broker of brokers) {
        await brokerPool.connectBroker(broker)
      }

      expect(brokerPool.hasConnectedBrokers()).toEqual(true)
      await brokerPool.disconnect()

      for (const broker of brokers) {
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

  describe('#removeBroker', () => {
    let host, port

    beforeEach(async () => {
      await brokerPool.connect()
      await brokerPool.refreshMetadata([topicName])
      expect(Object.values(brokerPool.brokers).length).toBeGreaterThan(1)

      const brokerUri = plainTextBrokers().shift()
      const [hostToRemove, portToRemove] = brokerUri.split(':')
      host = hostToRemove
      port = Number(portToRemove)
    })

    it('removes the broker by host and port', () => {
      const numberOfBrokers = Object.values(brokerPool.brokers).length

      brokerPool.removeBroker({ host, port })

      const brokers = Object.values(brokerPool.brokers)
      expect(brokers.length).toEqual(numberOfBrokers - 1)
      expect(
        brokers.find(
          broker => broker.connectionPool.host === host && broker.connectionPool.port === port
        )
      ).toEqual(undefined)
    })

    it('replaces the seed broker if it is the target broker', () => {
      const seedBrokerHost = brokerPool.seedBroker.connectionPool.host
      const seedBrokerPort = brokerPool.seedBroker.connectionPool.port
      brokerPool.removeBroker({ host: seedBrokerHost, port: seedBrokerPort })

      // check only port since the host will be "localhost" on most tests
      expect(brokerPool.seedBroker.connectionPool.port).not.toEqual(seedBrokerPort)
    })

    it('erases metadataExpireAt to force a metadata refresh', () => {
      brokerPool.metadataExpireAt = Date.now() + 25
      brokerPool.removeBroker({ host, port })
      expect(brokerPool.metadataExpireAt).toEqual(null)
    })
  })

  describe('#hasConnectedBrokers', () => {
    it('returns true if the seed broker is connected', async () => {
      expect(brokerPool.hasConnectedBrokers()).toEqual(false)
      await brokerPool.connect()
      expect(brokerPool.hasConnectedBrokers()).toEqual(true)
    })

    it('returns true if any of the brokers are connected', async () => {
      expect(brokerPool.hasConnectedBrokers()).toEqual(false)
      await brokerPool.connect()
      await brokerPool.refreshMetadata([topicName])

      const broker = Object.values(brokerPool.brokers).find(broker => !broker.isConnected())
      expect(broker).not.toEqual(brokerPool.seedBroker)

      await broker.connect()
      await brokerPool.seedBroker.disconnect()

      expect(brokerPool.hasConnectedBrokers()).toEqual(true)
    })

    it('returns false when nothing is connected', async () => {
      expect(brokerPool.hasConnectedBrokers()).toEqual(false)
      await brokerPool.connect()
      await brokerPool.disconnect()
      expect(brokerPool.hasConnectedBrokers()).toEqual(false)
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
      const seed = brokerPool.seedBroker.connectionPool
      const brokers = Object.values(brokerPool.brokers)
      const seedFromBrokerPool = brokers
        .map(b => b.connectionPool)
        .find(b => b.host === seed.host && b.port === seed.port)

      expect(seedFromBrokerPool).toEqual(seed)
    })

    it('cleans up unused brokers', async () => {
      await brokerPool.refreshMetadata([topicName])

      const nodeId = 'fakebroker'
      const fakeBroker = new Broker({
        connectionPool: createConnectionPool(),
        logger: newLogger(),
      })

      jest.spyOn(fakeBroker, 'disconnect')
      brokerPool.brokers[nodeId] = fakeBroker
      expect(Object.keys(brokerPool.brokers)).toEqual(['0', '1', '2', 'fakebroker'])

      await brokerPool.refreshMetadata([topicName])

      expect(fakeBroker.disconnect).toHaveBeenCalled()
      expect(Object.keys(brokerPool.brokers)).toEqual(['0', '1', '2'])
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

    it('retries on UNKNOWN_TOPIC_OR_PARTITION errors', async () => {
      const unknownTopicError = new KafkaJSProtocolError({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
        retriable: true,
        message: 'This server does not host this topic-partition',
      })

      brokerPool.findConnectedBroker = jest.fn(() => brokerPool.seedBroker)
      jest.spyOn(brokerPool.seedBroker, 'metadata').mockImplementationOnce(() => {
        throw unknownTopicError
      })

      expect(brokerPool.metadata).toEqual(null)
      await brokerPool.refreshMetadata([topicName])
      expect(brokerPool.metadata).not.toEqual(null)
    })

    describe(`without allowAutoTopicCreation`, () => {
      beforeEach(async () => {
        brokerPool && (await brokerPool.disconnect())
        topicName = `test-topic-${secureRandom()}`
        brokerPool = new BrokerPool({
          connectionPoolBuilder: createConnectionBuilder(),
          allowAutoTopicCreation: false,
          logger: newLogger(),
        })
        await brokerPool.connect()
      })

      afterEach(async () => {
        brokerPool && (await brokerPool.disconnect())
      })

      it('does not retry on UNKNOWN_TOPIC_OR_PARTITION errors', async () => {
        const unknownTopicError = new KafkaJSProtocolError({
          type: 'UNKNOWN_TOPIC_OR_PARTITION',
          code: 3,
          retriable: true,
          message: 'This server does not host this topic-partition',
        })

        brokerPool.findConnectedBroker = jest.fn(() => brokerPool.seedBroker)
        jest.spyOn(brokerPool.seedBroker, 'metadata').mockImplementationOnce(() => {
          throw unknownTopicError
        })

        expect(brokerPool.metadata).toEqual(null)
        await expect(brokerPool.refreshMetadata([topicName])).rejects.toEqual(unknownTopicError)
      })
    })

    describe('when replacing nodeIds with different host/port/rack', () => {
      let lastBroker

      beforeEach(async () => {
        await brokerPool.refreshMetadata([topicName])
        lastBroker = brokerPool.brokers[Object.keys(brokerPool.brokers).length - 1]
        jest.spyOn(brokerPool, 'findConnectedBroker').mockImplementation(() => lastBroker)
      })

      it('replaces the broker when the host change', async () => {
        jest.spyOn(lastBroker, 'metadata').mockImplementationOnce(() => ({
          ...brokerPool.metadata,
          brokers: brokerPool.metadata.brokers.map(broker =>
            broker.nodeId === 0 ? { ...broker, host: '0.0.0.0' } : broker
          ),
        }))

        await brokerPool.refreshMetadata([topicName])
        expect(brokerPool.brokers[0].connectionPool.host).toEqual('0.0.0.0')
      })

      it('replaces the broker when the port change', async () => {
        jest.spyOn(lastBroker, 'metadata').mockImplementationOnce(() => ({
          ...brokerPool.metadata,
          brokers: brokerPool.metadata.brokers.map(broker =>
            broker.nodeId === 0 ? { ...broker, port: 4321 } : broker
          ),
        }))

        await brokerPool.refreshMetadata([topicName])
        expect(brokerPool.brokers[0].connectionPool.port).toEqual(4321)
      })

      it('replaces the broker when the rack change', async () => {
        jest.spyOn(lastBroker, 'metadata').mockImplementationOnce(() => ({
          ...brokerPool.metadata,
          brokers: brokerPool.metadata.brokers.map(broker =>
            broker.nodeId === 0 ? { ...broker, rack: 'south-1' } : broker
          ),
        }))

        await brokerPool.refreshMetadata([topicName])
        expect(brokerPool.brokers[0].connectionPool.rack).toEqual('south-1')
      })
    })
  })

  describe('#refreshMetadataIfNecessary', () => {
    beforeEach(() => {
      brokerPool.refreshMetadata = jest.fn()
      brokerPool.metadataMaxAge = 1
      brokerPool.metadata = {
        topicMetadata: [],
      }
    })

    it('calls refreshMetadata if metadataExpireAt is not defined', async () => {
      brokerPool.metadataExpireAt = null
      await brokerPool.refreshMetadataIfNecessary([topicName])
      expect(brokerPool.refreshMetadata).toHaveBeenCalledWith([topicName])
    })

    it('calls refreshMetadata if metadata is not initialized', async () => {
      brokerPool.metadataExpireAt = Date.now() + 1000
      brokerPool.metadata = null
      await brokerPool.refreshMetadataIfNecessary([topicName])
      expect(brokerPool.refreshMetadata).toHaveBeenCalledWith([topicName])
    })

    it('calls refreshMetadata if metadata is expired', async () => {
      brokerPool.metadataExpireAt = Date.now() - 1000
      await brokerPool.refreshMetadataIfNecessary([topicName])
      expect(brokerPool.refreshMetadata).toHaveBeenCalledWith([topicName])
    })

    it('calls refreshMetadata if metadata is not present', async () => {
      brokerPool.metadataExpireAt = Date.now() + 1000
      await brokerPool.refreshMetadataIfNecessary([topicName])
      expect(brokerPool.refreshMetadata).toHaveBeenCalledWith([topicName])
    })

    it('does not call refreshMetadata if metadata is valid and up to date', async () => {
      brokerPool.metadataExpireAt = Date.now() + 1000
      brokerPool.metadata = {
        topicMetadata: [
          {
            topic: topicName,
          },
        ],
      }
      await brokerPool.refreshMetadataIfNecessary([topicName])
      expect(brokerPool.refreshMetadata).not.toHaveBeenCalled()
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

    it('recreates the connection on ILLEGAL_SASL_STATE error', async () => {
      const nodeId = 'fakebroker'
      const mockBroker = new Broker({
        connectionPool: createConnectionPool(),
        logger: newLogger(),
      })
      jest.spyOn(mockBroker, 'connect').mockImplementationOnce(() => {
        throw createErrorFromCode(errorCodes.find(({ type }) => type === 'ILLEGAL_SASL_STATE').code)
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
      for (const broker of Object.values(brokerPool.brokers)) {
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
