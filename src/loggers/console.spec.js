const MockDate = require('mockdate')

const { createLogger, LEVELS } = require('./index')
const LoggerConsole = require('./console')

describe('Loggers > Console', () => {
  let logger, timeNow

  beforeEach(() => {
    jest.spyOn(global.console, 'info').mockImplementation(() => true)
    jest.spyOn(global.console, 'error').mockImplementation(() => true)
    jest.spyOn(global.console, 'warn').mockImplementation(() => true)
    jest.spyOn(global.console, 'log').mockImplementation(() => true)

    timeNow = new Date('2017-12-29T14:15:38.572Z')
    MockDate.set(timeNow.getTime())

    logger = createLogger({ level: LEVELS.DEBUG, logCreator: LoggerConsole })
  })

  afterEach(() => {
    global.console.info.mockRestore()
    global.console.error.mockRestore()
    global.console.warn.mockRestore()
    global.console.log.mockRestore()
  })

  it('logs INFO', () => {
    logger.info('<info message>', { extra1: true })
    expect(console.info).toHaveBeenCalledWith(
      JSON.stringify({
        level: 'INFO',
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<info message>',
        extra1: true,
      })
    )
  })

  it('logs ERROR', () => {
    logger.error('<error message>', { extra1: true })
    expect(console.error).toHaveBeenCalledWith(
      JSON.stringify({
        level: 'ERROR',
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<error message>',
        extra1: true,
      })
    )
  })

  it('logs WARN', () => {
    logger.warn('<warn message>', { extra1: true })
    expect(console.warn).toHaveBeenCalledWith(
      JSON.stringify({
        level: 'WARN',
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<warn message>',
        extra1: true,
      })
    )
  })

  it('logs DEBUG', () => {
    logger.debug('<debug message>', { extra1: true })
    expect(console.log).toHaveBeenCalledWith(
      JSON.stringify({
        level: 'DEBUG',
        timestamp: timeNow.toISOString(),
        logger: 'kafkajs',
        message: '<debug message>',
        extra1: true,
      })
    )
  })

  describe('when the log level is NOTHING', () => {
    beforeEach(() => {
      logger = createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole })
    })

    it('does not log', () => {
      logger.info('<do not log info>', { extra1: true })
      logger.error('<do not log error>', { extra1: true })
      logger.warn('<do not log warn>', { extra1: true })
      logger.debug('<do not log debug>', { extra1: true })

      expect(console.info).not.toHaveBeenCalled()
      expect(console.error).not.toHaveBeenCalled()
      expect(console.warn).not.toHaveBeenCalled()
      expect(console.log).not.toHaveBeenCalled()
    })
  })

  describe('when namespace is present', () => {
    beforeEach(() => {
      const rootLogger = createLogger({ level: LEVELS.DEBUG, logCreator: LoggerConsole })
      logger = rootLogger.namespace('MyNamespace')
    })

    it('includes the namespace into the message', () => {
      logger.info('<namespace info>', { extra1: true })
      logger.error('<namespace error>', { extra1: true })
      logger.warn('<namespace warn>', { extra1: true })
      logger.debug('<namespace debug>', { extra1: true })

      expect(console.info).toHaveBeenCalledWith(
        JSON.stringify({
          level: 'INFO',
          timestamp: timeNow.toISOString(),
          logger: 'kafkajs',
          message: '[MyNamespace] <namespace info>',
          extra1: true,
        })
      )

      expect(console.error).toHaveBeenCalledWith(
        JSON.stringify({
          level: 'ERROR',
          timestamp: timeNow.toISOString(),
          logger: 'kafkajs',
          message: '[MyNamespace] <namespace error>',
          extra1: true,
        })
      )

      expect(console.warn).toHaveBeenCalledWith(
        JSON.stringify({
          level: 'WARN',
          timestamp: timeNow.toISOString(),
          logger: 'kafkajs',
          message: '[MyNamespace] <namespace warn>',
          extra1: true,
        })
      )

      expect(console.log).toHaveBeenCalledWith(
        JSON.stringify({
          level: 'DEBUG',
          timestamp: timeNow.toISOString(),
          logger: 'kafkajs',
          message: '[MyNamespace] <namespace debug>',
          extra1: true,
        })
      )
    })
  })
})
