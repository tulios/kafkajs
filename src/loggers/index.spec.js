const MockDate = require('mockdate')
const { createLogger, LEVELS } = require('./index')

describe('Loggers', () => {
  let logger, timeNow, MyLogCreator, myLogger

  beforeEach(() => {
    timeNow = new Date('2017-12-29T14:15:38.572Z')
    MockDate.set(timeNow.getTime())

    myLogger = jest.fn()
    MyLogCreator = jest.fn(logLevel => {
      return jest.fn(({ namespace, level, label, log }) => {
        myLogger(logLevel, { namespace, level, label, log })
      })
    })

    const rootLogger = createLogger({ logCreator: MyLogCreator, level: LEVELS.DEBUG })
    logger = rootLogger.namespace('MyNamespace')
  })

  it('calls the log creator with the log level', () => {
    expect(MyLogCreator).toHaveBeenCalledWith(LEVELS.DEBUG)
  })

  it('calls log function info with namespace, level, label, and log', () => {
    logger.info('<message info>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith(LEVELS.DEBUG, {
      namespace: 'MyNamespace',
      level: LEVELS.INFO,
      label: 'INFO',
      log: {
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<message info>',
        extra1: true,
      },
    })
  })

  it('calls log function error with namespace, level, label, and log', () => {
    logger.error('<message error>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith(LEVELS.DEBUG, {
      namespace: 'MyNamespace',
      level: LEVELS.ERROR,
      label: 'ERROR',
      log: {
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<message error>',
        extra1: true,
      },
    })
  })

  it('calls log function warn with namespace, level, label, and log', () => {
    logger.warn('<message warn>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith(LEVELS.DEBUG, {
      namespace: 'MyNamespace',
      level: LEVELS.WARN,
      label: 'WARN',
      log: {
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<message warn>',
        extra1: true,
      },
    })
  })

  it('calls log function debug with namespace, level, label, and log', () => {
    logger.debug('<message debug>', { extra1: true })
    expect(myLogger).toHaveBeenCalledWith(LEVELS.DEBUG, {
      namespace: 'MyNamespace',
      level: LEVELS.DEBUG,
      label: 'DEBUG',
      log: {
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<message debug>',
        extra1: true,
      },
    })
  })

  it('accepts a custom logLevel for the namespaced logger', () => {
    const newLogger = logger.namespace('Custom', LEVELS.NOTHING)
    newLogger.debug('<test custom logLevel for namespaced logger>')
    expect(myLogger).not.toHaveBeenCalled()
  })

  it('allows overriding the logLevel after instantiation', () => {
    logger.setLogLevel(LEVELS.INFO)

    logger.debug('Debug message that never gets logged')
    expect(myLogger).not.toHaveBeenCalled()

    logger.info('Info message', { extra: true })
    expect(myLogger).toHaveBeenCalledWith(LEVELS.DEBUG, {
      namespace: 'MyNamespace',
      level: LEVELS.INFO,
      label: 'INFO',
      log: {
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: 'Info message',
        extra: true,
      },
    })
  })
})
