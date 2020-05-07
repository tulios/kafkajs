jest.mock('./producer')
jest.mock('./consumer')
jest.mock('./admin')
jest.mock('./cluster')

const { Kafka: Client } = require('../index')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const Cluster = require('./cluster')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')

describe('Client', () => {
  it('gives access to its logger', () => {
    expect(new Client({ brokers: [] }).logger()).toMatchSnapshot()
  })

  it('shares a commit mapping between the consumer and the producer', () => {
    const client = new Client({ brokers: [] })

    expect(Cluster).toHaveBeenCalledTimes(0)

    client.producer({})
    client.consumer({})

    expect(Cluster).toHaveBeenCalledTimes(2)
    expect(Cluster.mock.calls[0][0].offsets).toBeInstanceOf(Map)
    expect(Cluster.mock.calls[0][0].offsets).toBe(Cluster.mock.calls[1][0].offsets)

    expect(createProducer.mock.calls[0][0].cluster).toBe(Cluster.mock.instances[0])
    expect(createConsumer.mock.calls[0][0].cluster).toBe(Cluster.mock.instances[1])
  })

  it('passes options to the producer', () => {
    const client = new Client({ brokers: [] })
    const options = {
      cluster: expect.any(Object),
      logger: expect.any(Object),
      createPartitioner: () => 0,
      retry: { retries: 10 },
      idempotent: true,
      transactionalId: 'transactional-id',
      transactionTimeout: 1,
      instrumentationEmitter: expect.any(Object),
    }
    client.producer(options)

    expect(createProducer).toHaveBeenCalledWith(options)
  })

  describe('consumer', () => {
    test('creates a consumer with the correct isolation level', () => {
      const client = new Client({ brokers: [] })

      const readCommittedConsumerOptions = {
        readUncommitted: false,
      }
      const readUncommittedConsumerOptions = {
        readUncommitted: true,
      }
      client.consumer(readCommittedConsumerOptions)

      expect(Cluster).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
      )
      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
      )

      createConsumer.mockClear()
      Cluster.mockClear()
      client.consumer(readUncommittedConsumerOptions)

      expect(Cluster).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        })
      )
      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        })
      )
    })
  })

  describe('retry configurations', () => {
    it('merges local producer options with the client options', () => {
      const client = new Client({ retry: { initialRetryTime: 100 } })
      client.producer({ retry: { multiplier: 3 } })

      expect(createProducer).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { initialRetryTime: 100, multiplier: 3 },
        })
      )
    })

    it('merges local consumer options with the client options', () => {
      const client = new Client({ retry: { initialRetryTime: 100 } })
      client.consumer({ retry: { multiplier: 3 } })

      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { initialRetryTime: 100, multiplier: 3 },
        })
      )
    })

    it('creates consumer with the default options', () => {
      const client = new Client({})
      client.consumer()

      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { retries: 5 },
        })
      )
    })

    it('merges local admin options with the client options', () => {
      const client = new Client({ retry: { initialRetryTime: 100 } })
      client.admin({ retry: { multiplier: 3 } })

      expect(createAdmin).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { initialRetryTime: 100, multiplier: 3 },
        })
      )
    })
  })
})
