const { createLogger, LEVELS: { DEBUG } } = require('./loggers/console')
const Connection = require('./connection')
const loadAPI = require('./api')

const logger = createLogger({ level: DEBUG })
const connection = new Connection({ host: 'localhost', port: 9092, logger })

connection
  .connect()
  .then(async () => {
    const api = await loadAPI(connection)

    const r1 = await api.metadata(['topic1'])
    logger.info(JSON.stringify(r1))

    const topicData = [
      {
        topic: 'topic1',
        partitions: [
          {
            partition: 0,
            messages: [
              { key: 'key1', value: 'some-value1' },
              { key: 'key2', value: 'some-value2' },
              { key: 'key3', value: 'some-value3' },
            ],
          },
        ],
      },
    ]

    const r2 = await api.produce({ topicData })
    logger.info(JSON.stringify(r2))
  })
  .catch(e => {
    logger.error(e.message, e)
    connection.disconnect()
  })

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['exit', 'SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      connection.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      connection.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
