const { newLogger } = require('testHelpers')
const createTransactionManager = require('./transactionManager')
const COORDINATOR_TYPES = require('../protocol/coordinatorTypes')

describe('Producer > transactionManager', () => {
  const topic = 'topic-name'
  const producerId = 1000
  const producerEpoch = 1
  const mockInitProducerIdResponse = {
    producerId,
    producerEpoch,
  }

  let cluster, broker

  beforeEach(() => {
    broker = {
      initProducerId: jest.fn().mockReturnValue(mockInitProducerIdResponse),
      addPartitionsToTxn: jest.fn(),
    }
    cluster = {
      refreshMetadataIfNecessary: jest.fn(),
      findGroupCoordinator: jest.fn().mockReturnValue(broker),
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
    expect(transactionManager.isInitialized()).toEqual(false)

    await transactionManager.initProducerId()

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
    expect(broker.initProducerId).toHaveBeenCalledWith({ transactionTimeout: 30000 })

    expect(transactionManager.getProducerId()).toEqual(mockInitProducerIdResponse.producerId)
    expect(transactionManager.getProducerEpoch()).toEqual(mockInitProducerIdResponse.producerEpoch)
    expect(transactionManager.isInitialized()).toEqual(true)
  })

  test('getting & updating the sequence per topic-partition', async () => {
    const transactionManager = createTransactionManager({ logger: newLogger(), cluster })

    expect(transactionManager.getSequence(topic, 1)).toEqual(0)
    transactionManager.updateSequence(topic, 1, 10) // No effect if we haven't initialized
    expect(transactionManager.getSequence(topic, 1)).toEqual(0)

    await transactionManager.initProducerId()

    expect(transactionManager.getSequence(topic, 1)).toEqual(0)
    transactionManager.updateSequence(topic, 1, 5)
    transactionManager.updateSequence(topic, 1, 10)
    expect(transactionManager.getSequence(topic, 1)).toEqual(15)

    expect(transactionManager.getSequence(topic, 2)).toEqual(0) // Different partition
    expect(transactionManager.getSequence('foobar', 1)).toEqual(0) // Different topic

    transactionManager.updateSequence(topic, 3, Math.pow(2, 32) - 100)
    expect(transactionManager.getSequence(topic, 3)).toEqual(Math.pow(2, 32) - 100) // Rotates once we reach 2 ^ 32 (max Int32)
    transactionManager.updateSequence(topic, 3, 100)
    expect(transactionManager.getSequence(topic, 3)).toEqual(0) // Rotates once we reach 2 ^ 32 (max Int32)

    await transactionManager.initProducerId()
    expect(transactionManager.getSequence(topic, 1)).toEqual(0) // Sequences reset by initProducerId
  })

  describe('if transactional=true', () => {
    let transactionalId

    beforeEach(() => {
      transactionalId = 'transactional-id'
    })

    test('adding partitions to transaction', async () => {
      const transactionManager = createTransactionManager({
        logger: newLogger(),
        cluster,
        transactionalId,
        transactional: true,
      })
      await transactionManager.initProducerId()
      transactionManager.beginTransaction()

      const topicData = [
        {
          topic: 'test-1',
          partitions: [{ partition: 1 }, { partition: 2 }],
        },
        {
          topic: 'test-2',
          partitions: [{ partition: 1 }],
        },
      ]

      cluster.findGroupCoordinator.mockClear()
      await transactionManager.addPartitionsToTransaction(topicData)

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(1)
      expect(broker.addPartitionsToTxn).toHaveBeenCalledWith({
        transactionalId,
        producerId,
        producerEpoch,
        topics: [
          {
            topic: 'test-1',
            partitions: [1, 2],
          },
          {
            topic: 'test-2',
            partitions: [1],
          },
        ],
      })

      broker.addPartitionsToTxn.mockClear()
      await transactionManager.addPartitionsToTransaction(topicData)
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(0) // No call if nothing new

      broker.addPartitionsToTxn.mockClear()
      await transactionManager.addPartitionsToTransaction([
        ...topicData,
        { topic: 'test-2', partitions: [{ partition: 2 }] },
        { topic: 'test-3', partitions: [{ partition: 1 }] },
      ])
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(1) // Called if some new
      expect(broker.addPartitionsToTxn).toHaveBeenCalledWith({
        transactionalId,
        producerId,
        producerEpoch,
        topics: [
          {
            topic: 'test-2',
            partitions: [2],
          },
          {
            topic: 'test-3',
            partitions: [1],
          },
        ],
      })
    })
  })
})
