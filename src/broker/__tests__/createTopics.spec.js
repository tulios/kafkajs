const { createConnection, connectionOpts, secureRandom, newLogger } = require('testHelpers')

const Broker = require('../index')
const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

describe('Broker > createTopics', () => {
  let seedBroker, broker

  beforeEach(async () => {
    seedBroker = new Broker({
      connection: createConnection(connectionOpts()),
      logger: newLogger(),
    })
    await seedBroker.connect()

    const metadata = await seedBroker.metadata()
    const newBrokerData = metadata.brokers.find(b => b.nodeId === metadata.controllerId)

    broker = new Broker({
      connection: createConnection(newBrokerData),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await broker.connect()
    const topicName1 = `test-topic-${secureRandom()}`
    const topicName2 = `test-topic-${secureRandom()}`
    const response = await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      topicErrors: [
        {
          topic: topicName1,
          errorCode: 0,
          errorMessage: null,
        },
        {
          topic: topicName2,
          errorCode: 0,
          errorMessage: null,
        },
      ].sort(topicNameComparator),
    })
  })

  test('request with validateOnly', async () => {
    await broker.connect()
    const topicName = `test-topic-${secureRandom()}`
    const response = await broker.createTopics({
      topics: [{ topic: topicName }],
      validateOnly: true,
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      topicErrors: [
        {
          topic: topicName,
          errorCode: 0,
          errorMessage: null,
        },
      ].sort(topicNameComparator),
    })
  })
})
