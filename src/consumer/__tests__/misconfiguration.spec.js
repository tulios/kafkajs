const createConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')

const { createCluster, newLogger } = require('testHelpers')

describe('Consumer', () => {
  it('throws when heartbeatInterval is lower or equal to sessionTimeout', () => {
    const errorMessage =
      'Consumer heartbeatInterval (10000) must be lower than sessionTimeout (10000). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.'

    expect(() =>
      createConsumer({
        cluster: createCluster(),
        logger: newLogger(),
        groupId: 'test-group-id',
        heartbeatInterval: 10000,
        sessionTimeout: 10000,
      })
    ).toThrowWithMessage(KafkaJSNonRetriableError, errorMessage)
  })

  it('throws when calling describeGroup for consumer without group', async () => {
    const consumer = createConsumer({
      cluster: createCluster(),
      logger: newLogger(),
    })

    await expect(consumer.describeGroup()).rejects.toHaveProperty(
      'message',
      'describeGroup is not supported for consumers without a group'
    )
  })
})
