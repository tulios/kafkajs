---
id: version-3.0.0-custom-logger
title: Custom Logger
original_id: custom-logger
---

The logger is customized using log creators. A log creator is a function which receives a log level and returns a log function. The log function receives namespace, level, label, and log.

- `namespace` identifies the component which is performing the log, for example, connection or consumer.
- `level` is the log level of the log entry.
- `label` is a text representation of the log level, example: 'INFO'.
- `log` is an object with the following keys: `timestamp`, `logger`, `message`, and the extra keys given by the user. (`logger.info('test', { extra_data: true })`)

## Log Levels

| Log Level  | Enum Value | Description |
|------------|------------|-------------|
| `NOTHING`  | `0`        | Disables all logging |
| `ERROR`    | `1`        | Only error messages |
| `WARN`     | `2`        | Warnings and errors |
| `INFO`     | `4`        | Informational messages, warnings, and errors **(default)** |
| `DEBUG`    | `5`        | All messages including debug information |

## Log Entry Structure

```javascript
{
    level: 4,
    label: 'INFO', // NOTHING, ERROR, WARN, INFO, or DEBUG
    timestamp: '2017-12-29T13:39:54.575Z',
    logger: 'kafkajs',
    message: 'Started',
    // ... any other extra key provided to the log function
}
```

## Log Creator Interface

The general structure looks like this:

```typescript
type logCreator = (logLevel: logLevel) => (entry: LogEntry) => void

interface LogEntry {
  namespace: string
  level: logLevel
  label: string
  log: LoggerEntryContent
}

interface LoggerEntryContent {
  readonly timestamp: string
  readonly message: string
  [key: string]: any
}
```

```javascript
const MyLogCreator = logLevel => ({ namespace, level, label, log }) => {
    // Example:
    // const { timestamp, logger, message, ...others } = log
    // console.log(`${label} [${namespace}] ${message} ${JSON.stringify(others)}`)
}
```

## Logger Instance API

Each KafkaJS component (client, producer, consumer, admin) exposes a `logger()` method that returns a `Logger` instance:

| method      | description                                              | signature |
|-------------|----------------------------------------------------------|-----------|
| info()      | Log at INFO level                                         | `(message: string, extra?: object) => void` |
| error()     | Log at ERROR level                                        | `(message: string, extra?: object) => void` |
| warn()      | Log at WARN level                                         | `(message: string, extra?: object) => void` |
| debug()     | Log at DEBUG level                                        | `(message: string, extra?: object) => void` |
| namespace() | Create a child logger with a sub-namespace                | `(namespace: string, logLevel?: logLevel) => Logger` |
| setLogLevel()| Override the log level for this logger                   | `(logLevel: logLevel) => void` |

## Example: Winston Integration

```javascript
const { logLevel } = require('kafkajs')
const winston = require('winston')
const toWinstonLogLevel = level => {
    switch(level) {
        case logLevel.ERROR:
        case logLevel.NOTHING:
            return 'error'
        case logLevel.WARN:
            return 'warn'
        case logLevel.INFO:
            return 'info'
        case logLevel.DEBUG:
            return 'debug'
    }
}

const WinstonLogCreator = logLevel => {
    const logger = winston.createLogger({
        level: toWinstonLogLevel(logLevel),
        transports: [
            new winston.transports.Console(),
            new winston.transports.File({ filename: 'myapp.log' })
        ]
    })

    return ({ namespace, level, label, log }) => {
        const { message, ...extra } = log
        logger.log({
            level: toWinstonLogLevel(level),
            message,
            extra,
        })
    }
}
```

## Example: Pino Integration

```javascript
const { logLevel } = require('kafkajs')
const pino = require('pino')

const toPinoLogLevel = level => {
    switch(level) {
        case logLevel.ERROR:
        case logLevel.NOTHING:
            return 'error'
        case logLevel.WARN:
            return 'warn'
        case logLevel.INFO:
            return 'info'
        case logLevel.DEBUG:
            return 'debug'
    }
}

const PinoLogCreator = logLevel => {
    const logger = pino({ level: toPinoLogLevel(logLevel) })

    return ({ namespace, level, label, log }) => {
        const { message, ...extra } = log
        logger[toPinoLogLevel(level)]({ ...extra, namespace }, message)
    }
}
```

## Configuring the Client

Once you have your log creator you can use the `logCreator` option to configure the client:

```javascript
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['kafka1:9092', 'kafka2:9092'],
    logLevel: logLevel.ERROR,
    logCreator: WinstonLogCreator
})
```

## Accessing Loggers After Instantiation

To get access to the namespaced logger of a consumer, producer, admin or root Kafka client after instantiation, you can use the `logger` method:

```javascript
const client = new Kafka( ... )
client.logger().info( ... )

const consumer = kafka.consumer( ... )
consumer.logger().info( ... )

const producer = kafka.producer( ... )
producer.logger().info( ... )

const admin = kafka.admin( ... )
admin.logger().info( ... )
```

## Overriding Log Level at Runtime

You can change the log level at runtime using `setLogLevel`:

```javascript
const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  logLevel: logLevel.ERROR
})

// Override per-component
kafka.logger().setLogLevel(logLevel.WARN)

const producer = kafka.producer()
producer.logger().setLogLevel(logLevel.INFO)

const consumer = kafka.consumer({ groupId: 'my-group' })
consumer.logger().setLogLevel(logLevel.DEBUG)

const admin = kafka.admin()
admin.logger().setLogLevel(logLevel.NOTHING)
```

## Environment Variable

The environment variable `KAFKAJS_LOG_LEVEL` can also be used and it has **precedence over the configuration in code**:

```sh
KAFKAJS_LOG_LEVEL=info node code.js
```

Valid values: `nothing`, `error`, `warn`, `info`, `debug`

## Creating Child Loggers

You can create namespaced child loggers using the `namespace` method:

```javascript
const logger = kafka.logger()
const childLogger = logger.namespace('my-module')
childLogger.info('Hello from my-module')
// Output: INFO [my-module] Hello from my-module

// Optionally set a different log level for the child
const debugLogger = logger.namespace('verbose-module', logLevel.DEBUG)
```
