const LEVELS = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5,
}

const createLevel = (label, level, currentLevel, loggerFunction) => (message, extra = {}) => {
  if (level > currentLevel) return
  const logData = Object.assign(
    {
      level: label,
      timestamp: new Date().toISOString(),
      message,
    },
    extra
  )

  loggerFunction(JSON.stringify(logData))
}

const createLogger = ({ level = LEVELS.INFO } = {}) => {
  const logLevel = parseInt(process.env.LOG_LEVEL, 10) || level
  return {
    info: createLevel('INFO', LEVELS.INFO, logLevel, console.info),
    error: createLevel('ERROR', LEVELS.ERROR, logLevel, console.error),
    warn: createLevel('WARN', LEVELS.WARN, logLevel, console.warn),
    debug: createLevel('DEBUG', LEVELS.DEBUG, logLevel, console.log),
  }
}

module.exports = {
  LEVELS,
  createLogger,
}
