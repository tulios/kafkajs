const createConsumer = require('../index')

const { createCluster, newLogger } = require('testHelpers')

describe('Consumer', () => {
  it('gives access to its logger', () => {
    expect(
      createConsumer({
        cluster: createCluster(),
        groupId: '',
        logger: newLogger(),
      }).logger()
    ).toMatchSnapshot()
  })
})
