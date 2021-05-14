---
id: custom-authentication-mechanism
title: Custom Authentication Mechanisms
---

To use an authentication mechanism that is not supported out of the box by KafkaJS,
custom authentication mechanisms can be introduced:

```js
const { AuthenticationMechanisms } = require('kafkajs')

AuthenticationMechanisms['CUSTOM_AUTHENTICATION'] = () => require('custom-authentication-mechanism')
```

Note that the value should be a function that returns your authentication mechanism. This
is to avoid having to initialize authentication mechanisms that are supported but not used.

`CUSTOM_AUTHENTICATION` needs to match the SASL mechanism configured in the `sasl.enabled.mechanisms`
property in `server.properties`. See the Kafka documentation for information on how to
configure your brokers.

## Writing a custom authentication mechanism

A custom authentication mechanism needs to fulfill the following interface:

```ts
type AuthenticationMechanismCreator<SaslOptions, ParseResult> = (params: {
  sasl?: SaslOptions,
  connection: { host: string, port: number },
  logger: Logger,
  saslAuthenticate: (handlers: { request: SaslAuthenticationRequest<SaslOptions>, response?: SaslAuthenticationResponse<ParseResult> }) => Promise<ParseResult>
}) => AuthenticationMechanism

type AuthenticationMechanism = {
  authenticate(): Promise<void>
}

type SaslAuthenticationRequest<SaslOptions> = {
  encode: () => Buffer | Promise<Buffer>
}

type SaslAuthenticationResponse<ParseResult> = {
  decode: (rawResponse: Buffer) => Buffer | Promise<Buffer>
  parse: (data: Buffer) => ParseResult
}
```

* `sasl` - Whatever is passed into the client constructor as `sasl`
* `connection` - The host and port that the client will authenticate against
* `logger` - A logger instance namespaced to the authentication mechanism
* `saslAuthenticate` - an async function to make [`SaslAuthenticate`](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate)
requests towards the broker. The `request` and `response` functions are used to encode the `auth_bytes` of the request, and to optionally
decode and parse the `auth_bytes` in the response. `response` can be omitted if no response `auth_bytes` are expected.

### Example

In this example we will create a custom authentication mechanism called `simon`. The general
flow will be:

1. Read `sasl.says` from the client configuration
2. Send a [`SaslAuthenticate`](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate)
request with the value of `sasl.says` as `auth_bytes`.
3. Read the response from the broker. If `sasl.says` starts with "Simon says", the response `auth_bytes`
should equal `sasl.says`, if it does not start with "Simon says", it should be an empty string.

```js
const SimonAuthenticator = ({ sasl, connection, logger, saslAuthenticate }) => {
    const INT32_SIZE = 4
  
    const request = {
      /**
       * Encodes the value for `auth_bytes` in SaslAuthenticate request
       * @see https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
       * 
       * In this example, we are just sending `sasl.says` as a string,
       * with the length of the string in bytes prepended as an int32
       **/
      encode: () => {
        const byteLength = Buffer.byteLength(sasl.says, 'utf8')
        const buf = Buffer.alloc(INT32_SIZE + byteLength)
        buf.writeUInt32BE(byteLength, 0)
        buf.write(sasl.says, INT32_SIZE, byteLength, 'utf8')
        return buf
      },
    }

    const response = {
      /**
       * Decodes the `auth_bytes` in SaslAuthenticate response
       * @see https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
       * 
       * This is essentially the reverse of `request.encode`, where
       * we read the length of the string as an int32 and then read
       * that many bytes
       */
      decode: rawData => {
        const byteLength = rawData.readInt32BE(0)
        return rawData.slice(INT32_SIZE, INT32_SIZE + byteLength)
      },

      /**
       * The return value from `response.decode` is passed into
       * this function, which is responsible for interpreting
       * the data. In this case, we just turn the buffer back
       * into a string
       */
      parse: data => {
        return data.toString()
      },
    }

    return {
      /**
       * This function is responsible for orchestrating the authentication flow.
       * Essentially we will send a SaslAuthenticate request with the
       * value of `sasl.says` to the broker, and expect to
       * get the same value back.
       * 
       * Other authentication methods may do any other operations they
       * like, but communication with the brokers goes through
       * the SaslAuthenticate request.
       */
      authenticate: async () => {
        if (sasl.says == null) {
          throw new Error('SASL Simon: Invalid "says"')
        }

        const { host, port } = connection
        const broker = `${host}:${port}`

        try {
          logger.info('Authenticate with SASL Simon', { broker })
          const authenticateResponse = await saslAuthenticate({ request, response })
  
          const saidSimon = sasl.says.startsWith("Simon says ")
          const expectedResponse = saidSimon ? says.says : ""
          if (authenticateResponse !== expectedResponse) {
              throw new Error("Mismatching response from broker")
          }

          logger.info('SASL Simon authentication successful', { broker })
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

## Usage in Typescript

When using a custom authentication mechanism, you will need to extend the type of
available authentication mechanisms. This can be done via
[Module Augmentation](https://www.typescriptlang.org/docs/handbook/declaration-merging.html#module-augmentation).

```ts
// kafkajs.d.ts
import { SASLMechanismOptionsMap } from "kafkajs";

declare module "kafkajs" {
    interface SASLMechanismOptionsMap {
        'simon': {
            says: string
        }
    }
}
```

The key is the mechanism name, and the value is the expected shape of the `sasl` options,
not including the `mechanism: string` field. In the example above, we expect the client
to be configured as such:

```ts
import {  KafkaConfig, AuthenticationMechanisms } from 'kafkajs'

AuthenticationMechanisms['simon'] = () => SimonAuthenticator

const config: KafkaConfig = {
  sasl: {
    simon: {
      says: 'Simon says authenticate me'
    }
  }
}
```