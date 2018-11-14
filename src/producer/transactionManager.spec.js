const { newLogger } = require('testHelpers')
const createTransactionManager = require('./transactionManager')

describe('Producer > transactionManager', () => {
  const topic = 'topic-name'
  const mockInitProducerIdResponse = {
    producerId: 1000,
    producerEpoch: 1,
  }

  let cluster, broker

  beforeEach(() => {
    broker = {
      initProducerId: jest.fn().mockReturnValue(mockInitProducerIdResponse),
    }
    cluster = {
      refreshMetadataIfNecessary: jest.fn(),
      findControllerBroker: jest.fn().mockReturnValue(broker),
    }
  })

  test('initializing the producer id and epoch', async () => {
    const transactionManager = createTransactionManager({
      logger: newLogger(),
      cluster,
      transactionTimeout: 30000,
    })

    expect(transactionManager.getProducerId()).toEqual(-1)
    expect(transactionManager.getProducerEpoch()).toEqual(0)
    expect(transactionManager.getSequence(topic, 1)).toEqual(0)

    await transactionManager.initProducerId()

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
    expect(broker.initProducerId).toHaveBeenCalledWith({ transactionTimeout: 30000 })

    expect(transactionManager.getProducerId()).toEqual(mockInitProducerIdResponse.producerId)
    expect(transactionManager.getProducerEpoch()).toEqual(mockInitProducerIdResponse.producerEpoch)
  })

  test('getting & updating the sequence per topic-partition', async () => {
    const transactionManager = createTransactionManager({ logger: newLogger(), cluster })

    expect(transactionManager.getSequence(topic, 1)).toEqual(0)
    transactionManager.updateSequence(topic, 1, 10) // No effect if we haven't initialized
    expect(transactionManager.getSequence(topic, 1)).toEqual(0)

    await transactionManager.initProducerId()

    expect(transactionManager.getSequence(topic, 1)).toEqual(0)
    transactionManager.updateSequence(topic, 1, 5)
    transactionManager.updateSequence(topic, 1, 10) // Updates, not increments
    expect(transactionManager.getSequence(topic, 1)).toEqual(10)

    expect(transactionManager.getSequence(topic, 2)).toEqual(0) // Different partition
    expect(transactionManager.getSequence('foobar', 1)).toEqual(0) // Different topic

    await transactionManager.initProducerId()
    expect(transactionManager.getSequence(topic, 1)).toEqual(0) // Sequences reset by initProducerId
  })
})
