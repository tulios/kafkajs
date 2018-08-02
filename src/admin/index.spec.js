const createAdmin = require('./index')
const { createCluster, newLogger } = require('testHelpers')

describe('Admin', () => {
  it('gives access to its logger', () => {
    expect(
      createAdmin({
        cluster: createCluster(),
        logger: newLogger(),
      }).logger()
    ).toMatchSnapshot()
  })
})
