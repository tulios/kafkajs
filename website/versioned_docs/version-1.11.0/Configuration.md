---
id: version-1.11.0-configuration
title: Client Configuration
original_id: configuration
---

The client must be configured with at least one broker. The brokers on the list are considered seed brokers and are only used to bootstrap the client and load initial metadata.

```javascript
const { Kafka } = require('kafkajs')

// Create the client with the broker list
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
})
```

## SSL

The `ssl` option can be used to configure the TLS sockets. The options are passed directly to [`tls.connect`](https://nodejs.org/api/tls.html#tls_tls_connect_options_callback) and used to create the TLS Secure Context, all options are accepted.

```javascript
const fs = require('fs')

new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('/my/custom/ca.crt', 'utf-8')],
    key: fs.readFileSync('/my/custom/client-key.pem', 'utf-8'),
    cert: fs.readFileSync('/my/custom/client-cert.pem', 'utf-8')
  },
})
```

Refer to [TLS create secure context](https://nodejs.org/dist/latest-v8.x/docs/api/tls.html#tls_tls_createsecurecontext_options) for more information. `NODE_EXTRA_CA_CERTS` can be used to add custom CAs. Use `ssl: true` if you don't have any extra configurations and want to enable SSL.

## <a name="sasl"></a> SASL

Kafka has support for using SASL to authenticate clients. The `sasl` option can be used to configure the authentication mechanism. Currently, KafkaJS supports `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, and `AWS` mechanisms.

### Options

| option                    | description                                                                                                                                                                             | default |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| authenticationTimeout     | Timeout in ms for authentication requests                                                                                                                                               | `1000`  |
| reauthenticationThreshold | When periodic reauthentication (`connections.max.reauth.ms`) is configured on the broker side, reauthenticate when `reauthenticationThreshold` milliseconds remain of session lifetime. | `10000` |

### PLAIN/SCRAM Example

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  // authenticationTimeout: 1000,
  // reauthenticationThreshold: 10000,
  sasl: {
    mechanism: 'plain', // scram-sha-256 or scram-sha-512
    username: 'my-username',
    password: 'my-password'
  },
})
```

### AWS IAM Example

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  // authenticationTimeout: 1000,
  // reauthenticationThreshold: 10000,
  sasl: {
    mechanism: 'aws',
    authorizationIdentity: 'AIDAIOSFODNN7EXAMPLE', // UserId or RoleId
    accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
    secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    sessionToken: 'WHArYt8i5vfQUrIU5ZbMLCbjcAiv/Eww6eL9tgQMJp6QFNEXAMPLETOKEN' // Optional
  },
})
```

For more information on the basics of IAM credentials and authentication, see the
[AWS Security Credentials - Access Keys](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys) page.

Use of this functionality requires
[STACK's Kafka AWS IAM LoginModule](https://github.com/STACK-Fintech/kafka-auth-aws-iam), or a
compatible alternative to be installed on all of the target brokers.

In the above example, the `authorizationIdentity` must be the `aws:userid` of the AWS IAM
identity. Typically, you can retrieve this value using the `aws iam get-user` or `aws iam get-role`
commands of the [AWS CLI toolkit](https://aws.amazon.com/cli/). The `aws:userid` is usually listed
as the `UserId` or `RoleId` property of the response.

You can also programmatically retrieve the `aws:userid` for currently available credentials with the
[AWS SDK's Security Token Service](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/STS.html).

A complete breakdown can be found in the IAM User Guide's
[Reference on Policy Variables](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html#policy-vars-infotouse).

### Use Encrypted Protocols

It is **highly recommended** that you use SSL for encryption when using `PLAIN` or `AWS`,
otherwise credentials will be transmitted in cleartext!

## Connection Timeout

Time in milliseconds to wait for a successful connection. The default value is: `1000`.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  connectionTimeout: 3000
})
```

## Request Timeout

Time in milliseconds to wait for a successful request. The default value is: `30000`.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  requestTimeout: 25000
})
```

## <a name="retry"></a> Default Retry

The `retry` option can be used to set the configuration of the retry mechanism, which is used to retry connections and API calls to Kafka (when using producers or consumers).

The retry mechanism uses a randomization function that grows exponentially.
[Detailed example](RetryDetailed.md)

If the max number of retries is exceeded the retrier will throw `KafkaJSNumberOfRetriesExceeded` and interrupt. Producers will bubble up the error to the user code; Consumers will wait the retry time attached to the exception (it will be based on the number of attempts) and perform a full restart.

__Available options:__

| option              | description                                                                                                             | default             |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------------- |
| maxRetryTime        | Maximum wait time for a retry in milliseconds                                                                           | `30000`             |
| initialRetryTime    | Initial value used to calculate the retry in milliseconds (This is still randomized following the randomization factor) | `300`               |
| factor              | Randomization factor                                                                                                    | `0.2`               |
| multiplier          | Exponential factor                                                                                                      | `2`                 |
| retries             | Max number of retries per call                                                                                          | `5`                 |
| maxInFlightRequests | Max number of requests that may be in progress at any time. If falsey then no limit.                                    | `null` _(no limit)_ |

Example:

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
})
```

## Logging

KafkaJS has a built-in `STDOUT` logger which outputs JSON. It also accepts a custom log creator which allows you to integrate your favorite logger library. There are 5 log levels available: `NOTHING`, `ERROR`, `WARN`, `INFO`, and `DEBUG`. `INFO` is configured by default.

##### Log level

```javascript
const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  logLevel: logLevel.ERROR
})
```

To override the log level after instantiation, call `setLogLevel` on the individual logger.

```javascript
const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  logLevel: logLevel.ERROR
})
kafka.logger().setLogLevel(logLevel.WARN)

const producer = kafka.producer(...)
producer.logger().setLogLevel(logLevel.INFO)

const consumer = kafka.consumer(...)
consumer.logger().setLogLevel(logLevel.DEBUG)

const admin = kafka.admin(...)
admin.logger().setLogLevel(logLevel.NOTHING)
```

The environment variable `KAFKAJS_LOG_LEVEL` can also be used and it has precedence over the configuration in code, example:

```sh
KAFKAJS_LOG_LEVEL=info node code.js
```

> Note: for more information on how to customize your logs, take a look at [Custom logging](CustomLogger.md)

## Custom socket factory

To allow for custom socket configurations, the client accepts an optional `socketFactory` property that will be used to construct
any socket.

`socketFactory` should be a function that returns an object compatible with [`net.Socket`](https://nodejs.org/api/net.html#net_class_net_socket) (see the [default implementation](https://github.com/tulios/kafkajs/tree/master/src/network/socketFactory.js)).

```javascript
const { Kafka } = require('kafkajs')

// Example socket factory setting a custom TTL
const net = require('net')
const tls = require('tls')

const myCustomSocketFactory = ({ host, port, ssl, onConnect }) => {
  const socket = ssl
    ? tls.connect(
        Object.assign({ host, port }, ssl),
        onConnect
      )
    : net.connect(
        { host, port },
        onConnect
      )

  socket.setKeepAlive(true, 30000)

  return socket
}

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  socketFactory: myCustomSocketFactory,
})
```
