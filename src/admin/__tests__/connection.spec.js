const createAdmin = require('../index')

const {
  sslConnectionOpts,
  saslConnectionOpts,
  saslSCRAM256ConnectionOpts,
  saslSCRAM512ConnectionOpts,
  saslOAuthBearerConnectionOpts,
  createCluster,
  sslBrokers,
  saslBrokers,
  newLogger,
  describeIfOauthbearerEnabled,
  describeIfOauthbearerDisabled,
} = require('testHelpers')

describe('Admin', () => {
  let admin

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  test('support SSL connections', async () => {
    const cluster = createCluster(sslConnectionOpts(), sslBrokers())
    admin = createAdmin({ cluster, logger: newLogger() })

    await admin.connect()
  })

  describeIfOauthbearerDisabled('when SASL PLAIN and SCRAM are configured', () => {
    test('support SASL PLAIN connections', async () => {
      const cluster = createCluster(saslConnectionOpts(), saslBrokers())
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.connect()
    })

    test('support SASL SCRAM 256 connections', async () => {
      const cluster = createCluster(saslSCRAM256ConnectionOpts(), saslBrokers())
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.connect()
    })

    test('support SASL SCRAM 512 connections', async () => {
      const cluster = createCluster(saslSCRAM512ConnectionOpts(), saslBrokers())
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.connect()
    })
  })

  describeIfOauthbearerEnabled('when SASL OAUTHBEARER is configured', () => {
    test('support SASL OAUTHBEARER connections', async () => {
      const cluster = createCluster(saslOAuthBearerConnectionOpts(), saslBrokers())
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.connect()
    })
  })
})
