const { SCRAM, DIGESTS } = require('./scram')

module.exports = class SCRAM256Authenticator extends SCRAM {
  constructor(connection, logger) {
    super(connection, logger.namespace('SCRAM256Authenticator'))
  }

  digestDefinition() {
    return DIGESTS.SHA256
  }
}
