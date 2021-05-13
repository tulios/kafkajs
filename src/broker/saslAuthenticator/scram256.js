const { SCRAM, DIGESTS } = require('./scram')

function SCRAM256Authenticator({ sasl, connection, logger, saslAuthenticate }) {
  const scram = new SCRAM(sasl, connection, logger, saslAuthenticate, DIGESTS.SHA256)

  return {
    authenticate: async () => scram.authenticate(),
  }
}

module.exports = SCRAM256Authenticator
