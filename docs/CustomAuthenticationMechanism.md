---
id: custom-authentication-mechanism
title: Custom Authentication Mechanisms
---

To use an authentication mechanism that is not supported out of the box by KafkaJS,
custom authentication mechanisms can be introduced:

```js
{ 
  sasl: { 
      mechanism: <mechanism name>,
      authenticationProvider: ({ host, port, logger, saslAuthenticate }) => { authenticate: () => Promise<void> }
  }
}
```

`<mechanism name>` needs to match the SASL mechanism configured in the `sasl.enabled.mechanisms`
property in `server.properties`. See the Kafka documentation for information on how to
configure your brokers.

## Writing a custom authentication mechanism

A custom authentication mechanism needs to fulfill the following interface:

```ts
type SaslAuthenticateArgs<ParseResult> = {
  request: SaslAuthenticationRequest
  response?: SaslAuthenticationResponse<ParseResult>
}

type AuthenticationProviderArgs = {
  host: string
  port: number
  logger: Logger
  saslAuthenticate: <ParseResult>(
    args: SaslAuthenticateArgs<ParseResult>
  ) => Promise<ParseResult | void>
}

type Mechanism = {
  mechanism: string
  authenticationProvider: (args: AuthenticationProviderArgs) => Authenticator
}

type Authenticator = {
  authenticate(): Promise<void>
}

type SaslAuthenticationRequest = {
  encode: () => Buffer | Promise<Buffer>
}

type SaslAuthenticationResponse<ParseResult> = {
  decode: (rawResponse: Buffer) => Buffer | Promise<Buffer>
  parse: (data: Buffer) => ParseResult
}
```
* `host` - Hostname of the specific broker to connect to
* `port` - Port of the specific broker to connect to
* `logger` - A logger instance namespaced to the authentication mechanism
* `saslAuthenticate` - an async function to make [`SaslAuthenticate`](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate)
requests towards the broker. The `request` and `response` functions are used to encode the `auth_bytes` of the request, and to optionally
decode and parse the `auth_bytes` in the response. `response` can be omitted if no response `auth_bytes` are expected.
### Example
In this example we will create a custom authentication mechanism called `simon`. The general
flow will be:
1. Send a [`SaslAuthenticate`](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate)
request with the value of `says` as `auth_bytes`.
2. Read the response from the broker. If `says` starts with "Simon says", the response `auth_bytes`
should equal `says`, if it does not start with "Simon says", it should be an empty string.

**This is a made up example!**

It is a non-existent authentication mechanism just made up to show how to implement the expected interface. It is not a real authentication mechanism. 

```js
const simonAuthenticator = says = ({ host, port, logger, saslAuthenticate }) => {
    const INT32_SIZE = 4
  
    const request = {
      /**
       * Encodes the value for `auth_bytes` in SaslAuthenticate request
       * @see https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
       * 
       * In this example, we are just sending `says` as a string,
       * with the length of the string in bytes prepended as an int32
       **/
      encode: () => {
        const byteLength = Buffer.byteLength(says, 'utf8')
        const buf = Buffer.alloc(INT32_SIZE + byteLength)
        buf.writeUInt32BE(byteLength, 0)
        buf.write(says, INT32_SIZE, byteLength, 'utf8')
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
        if (says == null) {
          throw new Error('SASL Simon: Invalid "says"')
        }
        const broker = `${host}:${port}`
        try {
          logger.info('Authenticate with SASL Simon', { broker })
          const authenticateResponse = await saslAuthenticate({ request, response })
  
          const saidSimon = says.startsWith("Simon says ")
          const expectedResponse = saidSimon ? says : ""
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

In the example above, we expect the client to be configured as such:

```js
const config = {
  sasl: {
    mechanism: 'simon'
    authenticationProvider: simonAuthenticator('Simon says authenticate me')
  }
}
```