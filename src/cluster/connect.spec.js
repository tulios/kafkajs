const { createCluster } = require('../../testHelpers')
const { KafkaJSProtocolError } = require('../errors')

describe('Cluster > connect', () => {
  let cluster

  beforeEach(() => {
    cluster = createCluster()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('connect the seed broker', async () => {
    expect(cluster.seedBroker.isConnected()).toEqual(false)
    await cluster.connect()
    expect(cluster.seedBroker.isConnected()).toEqual(true)
  })

  test('does not connect again if still connected', async () => {
    cluster.seedBroker.connect = jest.fn(() => {
      cluster.seedBroker.isConnected = jest.fn(() => true)
    })

    expect(cluster.seedBroker.connect).not.toHaveBeenCalled()

    await cluster.connect()
    expect(cluster.seedBroker.connect).toHaveBeenCalledTimes(1)

    await cluster.connect()
    await cluster.connect()

    expect(cluster.seedBroker.connect).toHaveBeenCalledTimes(1)
  })

  test('load the versions from the seed broker', async () => {
    expect(cluster.versions).toEqual(null)
    await cluster.connect()
    expect(cluster.versions).toEqual(cluster.seedBroker.versions)
  })

  test('select a different seed broker on ILLEGAL_SASL_STATE error', async () => {
    const originalSeedPort = cluster.seedBroker.connection.port
    const illegalStateError = new KafkaJSProtocolError({
      message: 'ILLEGAL_SASL_STATE',
      type: 'ILLEGAL_SASL_STATE',
      code: 34,
    })

    cluster.seedBroker.connect = jest.fn(() => {
      throw illegalStateError
    })

    await cluster.connect()
    expect(cluster.seedBroker.connection.port).not.toEqual(originalSeedPort)
  })

  describe('#isConnected', () => {
    test('returns false when the seed broker is not connected', () => {
      cluster.seedBroker.isConnected = jest.fn(() => false)
      expect(cluster.isConnected()).toEqual(false)
    })

    test('returns true when the seed broker is connected', async () => {
      cluster.seedBroker.isConnected = jest.fn(() => true)
      expect(cluster.isConnected()).toEqual(true)
    })
  })
})
