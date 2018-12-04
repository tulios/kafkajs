const { SCRAM, DIGESTS } = require('./scram')

module.exports = class SCRAM512Authenticator extends SCRAM {
  constructor(connection, logger, saslAuthenticate) {
    super(connection, logger.namespace('SCRAM512Authenticator'), saslAuthenticate, DIGESTS.SHA512)
  }
}
