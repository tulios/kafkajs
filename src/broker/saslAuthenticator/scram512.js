const { SCRAM, DIGESTS } = require('./scram')

module.exports = class SCRAM512Authenticator extends SCRAM {
  constructor(connection, logger) {
    super(connection, logger.namespace('SCRAM512Authenticator'), DIGESTS.SHA512)
  }
}
