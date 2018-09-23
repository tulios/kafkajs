const { createConnection, connectionOpts, secureRandom, newLogger } = require('testHelpers')
const RESOURCE_TYPES = require('../../protocol/resourceTypes')
const Broker = require('../index')

describe('Broker > describeConfigs', () => {
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

    await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    const response = await broker.describeConfigs({
      resources: [
        {
          type: RESOURCE_TYPES.TOPIC,
          name: topicName1,
          configNames: ['compression.type', 'retention.ms'],
        },
      ],
    })

    expect(response).toEqual({
      resources: [
        {
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              isDefault: true,
              isSensitive: false,
              readOnly: false,
            },
            {
              configName: 'retention.ms',
              configValue: '604800000',
              isDefault: true,
              isSensitive: false,
              readOnly: false,
            },
          ],
          errorCode: 0,
          errorMessage: null,
          resourceName: topicName1,
          resourceType: RESOURCE_TYPES.TOPIC,
        },
      ],
      throttleTime: 0,
    })
  })
})
