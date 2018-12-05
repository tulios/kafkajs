const { SCRAM, DIGESTS } = require('./scram')

module.exports = class SCRAM256Authenticator extends SCRAM {
  constructor(connection, logger, saslAuthenticate) {
    super(connection, logger.namespace('SCRAM256Authenticator'), saslAuthenticate, DIGESTS.SHA256)
  }
}
