const createConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')

const { createCluster, newLogger } = require('testHelpers')

describe('Consumer', () => {
  it('throws when heartbeatInterval is lower or equal to sessionTimeout', () => {
    expect(() =>
      createConsumer({
        cluster: createCluster(),
        logger: newLogger(),
        groupId: '',
        heartbeatInterval: 10000,
        sessionTimeout: 10000,
      })
    ).toThrow(KafkaJSNonRetriableError)
  })
})
