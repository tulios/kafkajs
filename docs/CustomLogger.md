---
id: custom-logger
title: Custom Logger
---

The logger is customized using log creators. A log creator is a function which receives a log level and returns a log function. The log function receives namespace, level, label, and log.

- `namespace` identifies the component which is performing the log, for example, connection or consumer.
- `level` is the log level of the log entry.
- `label` is a text representation of the log level, example: 'INFO'.
- `log` is an object with the following keys: `timestamp`, `logger`, `message`, and the extra keys given by the user. (`logger.info('test', { extra_data: true })`)

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

The general structure looks like this:

```javascript
const MyLogCreator = logLevel => ({ namespace, level, label, log }) => {
    // Example:
    // const { timestamp, logger, message, ...others } = log
    // console.log(`${label} [${namespace}] ${message} ${JSON.stringify(others)}`)
}
```

Example using [Winston](https://github.com/winstonjs/winston):

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

Once you have your log creator you can use the `logCreator` option to configure the client:

```javascript
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['kafka1:9092', 'kafka2:9092'],
    logLevel: logLevel.ERROR,
    logCreator: WinstonLogCreator
})
```

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
