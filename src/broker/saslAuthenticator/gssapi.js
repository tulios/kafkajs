/**
 * GSSAPI (Kerberos) SASL Authentication
 *
 * Implements the SASL/GSSAPI mechanism for Kerberos authentication.
 * Requires the optional `kerberos` npm package to be installed.
 *
 * The authentication flow:
 * 1. Initialize a Kerberos client with the service principal
 * 2. Generate an initial token (AP-REQ)
 * 3. Send the token to the broker and receive a response (AP-REP)
 * 4. Continue the handshake if the context is not complete
 * 5. Wrap/unwrap the security layer negotiation
 *
 * @see https://kafka.apache.org/documentation/#security_sasl_kerberos
 * @see https://tools.ietf.org/html/rfc4752
 */

const { KafkaJSSASLAuthenticationError } = require('../../errors')

let kerberos
try {
  kerberos = require('kerberos') // eslint-disable-line
} catch (_) {
  // kerberos is an optional dependency
}

const gssapiAuthenticatorProvider = sasl => ({ host, port, logger, saslAuthenticate }) => {
  return {
    authenticate: async () => {
      if (!kerberos) {
        throw new KafkaJSSASLAuthenticationError(
          'SASL GSSAPI: The "kerberos" npm package is required for Kerberos authentication. ' +
            'Install it with: npm install kerberos'
        )
      }

      const { serviceName = 'kafka', principal, keytab, kerberosServicePrincipal } = sasl

      // Build the service principal: serviceName/host@REALM or use the provided one
      const spn = kerberosServicePrincipal || `${serviceName}/${host}`

      const broker = `${host}:${port}`

      try {
        logger.debug('Authenticate with SASL GSSAPI (Kerberos)', { broker })

        // Set KRB5_CLIENT_KTNAME if keytab is provided
        if (keytab) {
          process.env.KRB5_CLIENT_KTNAME = keytab
        }

        // Initialize the Kerberos client
        const initOptions = {}
        if (principal) {
          initOptions.principal = principal
        }

        const client = await kerberos.initializeClient(spn, initOptions)

        // Step 1: Generate initial token
        let token = await client.step('')

        // Step 2: Send token to broker and continue handshake
        while (!client.contextComplete) {
          const responseToken = await saslAuthenticate({
            request: {
              encode: async () => Buffer.from(token, 'base64'),
            },
            response: {
              decode: async rawData => rawData,
              parse: async data => data.toString('base64'),
            },
          })

          if (responseToken) {
            token = await client.step(responseToken)
          } else {
            break
          }
        }

        // Step 3: Security layer negotiation (wrap/unwrap)
        // The server sends a wrapped message with the security layer and max buffer size
        const wrappedChallenge = await saslAuthenticate({
          request: {
            encode: async () => (token ? Buffer.from(token, 'base64') : Buffer.alloc(0)),
          },
          response: {
            decode: async rawData => rawData,
            parse: async data => data.toString('base64'),
          },
        })

        if (wrappedChallenge) {
          // Unwrap the server's challenge
          const unwrapped = await client.unwrap(wrappedChallenge)
          const unwrappedBuf = Buffer.from(unwrapped, 'base64')

          // Parse the security layer bitmask and max buffer size
          // First byte: security layer bitmask
          // Bytes 2-4: max buffer size (big-endian)
          const serverSecurityLayer = unwrappedBuf[0]
          const maxBufferSize = unwrappedBuf.readUIntBE(1, 3)

          // We select no security layer (just authentication) with the same max buffer size
          const clientResponse = Buffer.alloc(4)
          // Use no security layer (0x01 = no security layer)
          clientResponse[0] = serverSecurityLayer & 0x01 ? 0x01 : serverSecurityLayer
          // Set max buffer size
          clientResponse.writeUIntBE(Math.min(maxBufferSize, 0xffffff), 1, 3)

          // If principal is available, append it
          const principalBytes = principal ? Buffer.from(principal) : Buffer.alloc(0)
          const responsePayload = Buffer.concat([clientResponse, principalBytes])

          // Wrap and send the response
          const wrappedResponse = await client.wrap(responsePayload.toString('base64'))

          await saslAuthenticate({
            request: {
              encode: async () => Buffer.from(wrappedResponse, 'base64'),
            },
          })
        }

        logger.debug('SASL GSSAPI authentication successful', { broker })
      } catch (e) {
        const error = new KafkaJSSASLAuthenticationError(
          `SASL GSSAPI authentication failed: ${e.message}`
        )
        logger.error(error.message, { broker })
        throw error
      }
    },
  }
}

module.exports = gssapiAuthenticatorProvider
