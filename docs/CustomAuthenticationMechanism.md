---
id: custom-authentication-mechanism
title: Custom Authentication Mechanisms
---

To use an authentication mechanism that is not supported out of the box by KafkaJS,
custom authentication mechanisms can be introduced:

```js
const CustomAuthenticationMechanism = require('custom-authentication-mechanism')
const { AuthenticationMechanisms } = require('kafkajs')

AuthenticationMechanisms['CUSTOM_AUTHENTICATION'] = CustomAuthenticationMechanism
```

`CUSTOM_AUTHENTICATION` needs to match the SASL mechanism configured in the `sasl.enabled.mechanisms`
property in `server.properties`. See the Kafka documentation for information on how to
configure your brokers.

## Writing a custom authentication mechanism

A custom authentication mechanism needs to fulfill the following interface:

```ts
type AuthenticationMechanismCreator = (params: {
  sasl: Record<string, any>,
  connection: { host: string, port: number },
  logger: Logger,
  saslAuthenticate: (handlers: { request: SaslAuthenticationRequest, response?: SaslAuthenticationResponse }) => Promise<void>
}) => AuthenticationMechanism

type AuthenticationMechanism = {
  authenticate(): Promise<void>
}

type SaslAuthenticationRequest = (sasl: Record<string, any>) => {
  encode: () => Buffer
}

type SaslAuthenticationResponse = () => {
  decode: (rawResponse: Buffer) => Buffer
  parse: (data: Buffer) => any
}
```

* `sasl` - Whatever is passed into the client constructor as `sasl`
* `connection` - The host and port that the client will authenticate against
* `logger` - A logger instance namespaced to the authentication mechanism
* `saslAuthenticate` - an async function to make [`SaslAuthenticate](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate)
requests towards the broker.

### Example

In this example we will create a custom authentication mechanism called `SIMON`. The general
flow will be:

1. Read `sasl.simonSays` from the client configuration
2. Send a [`SaslAuthenticate`](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate)
request with the value of `sasl.simonSays` as `auth_bytes`.
3. Read the response from the broker and validate that it returned the same value.

```js
function SimonAuthenticator({ sasl, connection, logger, saslAuthenticate }) {
  const INT32_SIZE = 4

  const request = ({
    /**
     * Encodes the value for `auth_bytes` in SaslAuthenticate request
     * @see https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
     * 
     * In this example, we are just sending `sasl.simonSays` as a string,
     * with the length of the string in bytes prepended as an int32
     **/
    encode: async () => {
      const byteLength = Buffer.byteLength(sasl.simonSays, 'utf8')
      const buf = Buffer.alloc(INT32_SIZE + byteLength)
      buf.writeUInt32BE(byteLength, 0)
      buf.write(sasl.simonSays, INT32_SIZE, byteLength, 'utf8')
      return buf
    },
  })

  const response = {
    /**
     * Decodes the `auth_bytes` in SaslAuthenticate response
     * @see https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
     * 
     * This is essentially the reverse of `request.encode`, where
     * we read the length of the string as an int32 and then read
     * that many bytes
     */
    decode: async rawData => {
      const byteLength = rawData.readInt32BE(0)
      return rawData.slice(INT32_SIZE, INT32_SIZE + byteLength)
    },

    /**
     * The return value from `response.decode` is passed into
     * this function, which is responsible for interpreting
     * the data. In this case, we just turn the buffer back
     * into a string
     */
    parse: async data => {
      return data.toString()
    },
  }

  return {
    /**
     * This function is responsible for orchestrating the authentication flow.
     * Essentially we will send a SaslAuthenticate request with the
     * value of `sasl.simonSays` to the broker, and expect to
     * get the same value back.
     * 
     * Other authentication methods may do any other operations they
     * like, but communication with the brokers goes through
     * the SaslAuthenticate request.
     */
    authenticate: async () => {
      if (sasl.simonSays == null) {
        throw new Error('SASL Simon: Invalid "simonSays"')
      }

      const { host, port } = connection
      const broker = `${host}:${port}`

      try {
        logger.debug('Authenticate with SASL Simon', { broker })
        const authenticateResponse = await saslAuthenticate({ request, response })

        if (authenticateResponse !== sasl.simonSays) {
            throw new Error("Mismatching response from broker")
        }

        logger.debug('SASL Simon authentication successful', { broker })
      } catch (e) {
        const error = new Error(
          `SASL Simon authentication failed: ${e.message}`
        )
        logger.error(error.message, { broker })
        throw error
      }
    },
  }
}
```

The `response` argument to `saslAuthenticate` is optional, in case the authentication
method does not require the `auth_bytes` in the response.

