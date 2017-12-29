const MockDate = require('mockdate')
const { createLogger, LEVELS } = require('./index')

describe('Loggers', () => {
  let logger, timeNow, MyLogCreator, myLogger

  beforeEach(() => {
    timeNow = new Date('2017-12-29T14:15:38.572Z')
    MockDate.set(timeNow.getTime())

    myLogger = jest.fn()
    MyLogCreator = jest.fn(logLevel => {
      return jest.fn((namespace, log) => {
        myLogger(logLevel, namespace, log)
      })
    })

    const rootLogger = createLogger({ logCreator: MyLogCreator, level: LEVELS.DEBUG })
    logger = rootLogger.namespace('MyNamespace')
  })

  it('calls the log creator with the log level', () => {
    expect(MyLogCreator).toHaveBeenCalledWith('DEBUG')
  })

  it('calls log function info with namespace and log', () => {
    logger.info('<message info>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith('DEBUG', 'MyNamespace', {
      level: 'INFO',
      timestamp: timeNow.toISOString(),
      logger: 'kafkajs',
      message: '<message info>',
      extra1: true,
    })
  })

  it('calls log function error with namespace and log', () => {
    logger.error('<message error>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith('DEBUG', 'MyNamespace', {
      level: 'ERROR',
      timestamp: timeNow.toISOString(),
      logger: 'kafkajs',
      message: '<message error>',
      extra1: true,
    })
  })

  it('calls log function warn with namespace and log', () => {
    logger.warn('<message warn>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith('DEBUG', 'MyNamespace', {
      level: 'WARN',
      timestamp: timeNow.toISOString(),
      logger: 'kafkajs',
      message: '<message warn>',
      extra1: true,
    })
  })

  it('calls log function debug with namespace and log', () => {
    logger.debug('<message debug>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith('DEBUG', 'MyNamespace', {
      level: 'DEBUG',
      timestamp: timeNow.toISOString(),
      logger: 'kafkajs',
      message: '<message debug>',
      extra1: true,
    })
  })
})
