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

const createLogger = ({ level = parseInt(process.env.LOG_LEVEL, 10) || LEVELS.INFO } = {}) => ({
  info: createLevel('INFO', LEVELS.INFO, level, console.info),
  error: createLevel('ERROR', LEVELS.ERROR, level, console.error),
  warn: createLevel('WARN', LEVELS.WARN, level, console.warn),
  debug: createLevel('DEBUG', LEVELS.DEBUG, level, console.log),
})

module.exports = {
  LEVELS,
  createLogger,
}
