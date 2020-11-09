const { createConnection, connectionOpts, secureRandom, newLogger } = require('testHelpers')
const RESOURCE_TYPES = require('../../protocol/resourceTypes')
const Broker = require('../index')

describe('Broker > alterConfigs', () => {
  let seedBroker, broker

  const getConfigEntries = response =>
    response.resources.find(r => r.resourceType === RESOURCE_TYPES.TOPIC).configEntries

  const getConfigValue = (configEntries, name) =>
    configEntries.find(c => c.configName === name).configValue

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

    const CONFIG_NAME = 'cleanup.policy'
    let describeResponse = await broker.describeConfigs({
      resources: [
        {
          type: RESOURCE_TYPES.TOPIC,
          name: topicName1,
          configNames: [CONFIG_NAME],
        },
      ],
    })

    let cleanupPolicy = getConfigValue(getConfigEntries(describeResponse), CONFIG_NAME)

    expect(cleanupPolicy).toEqual('delete')

    const response = await broker.alterConfigs({
      resources: [
        {
          type: RESOURCE_TYPES.TOPIC,
          name: topicName1,
          configEntries: [
            {
              name: 'cleanup.policy',
              value: 'compact',
            },
          ],
        },
      ],
    })

    expect(response).toEqual({
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceName: topicName1,
          resourceType: RESOURCE_TYPES.TOPIC,
        },
      ],
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
    })

    describeResponse = await broker.describeConfigs({
      resources: [
        {
          type: RESOURCE_TYPES.TOPIC,
          name: topicName1,
          configNames: [CONFIG_NAME],
        },
      ],
    })

    cleanupPolicy = getConfigValue(getConfigEntries(describeResponse), CONFIG_NAME)
    expect(cleanupPolicy).toEqual('compact')
  })
})
