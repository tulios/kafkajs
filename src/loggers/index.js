const { keys } = Object

const LEVELS = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5,
}

const createLevel = (label, level, currentLevel, namespace, loggerFunction) => (
  message,
  extra = {}
) => {
  if (level > currentLevel) return
  loggerFunction(
    namespace,
    Object.assign(
      {
        level: label,
        timestamp: new Date().toISOString(),
        logger: 'kafkajs',
        message,
      },
      extra
    )
  )
}

const createLogger = ({ level = LEVELS.INFO, logCreator = null } = {}) => {
  const envLogLevel = (process.env.KAFKAJS_LOG_LEVEL || '').toUpperCase()
  const logLevel = LEVELS[envLogLevel] || level
  const logLevelLabel = keys(LEVELS).find(k => LEVELS[k] === logLevel)
  const logFunction = logCreator(logLevelLabel)

  const createLogFunctions = namespace => ({
    info: createLevel('INFO', LEVELS.INFO, logLevel, namespace, logFunction),
    error: createLevel('ERROR', LEVELS.ERROR, logLevel, namespace, logFunction),
    warn: createLevel('WARN', LEVELS.WARN, logLevel, namespace, logFunction),
    debug: createLevel('DEBUG', LEVELS.DEBUG, logLevel, namespace, logFunction),
  })

  return Object.assign(createLogFunctions(), {
    namespace: namespace => createLogFunctions(namespace),
  })
}

module.exports = {
  LEVELS,
  createLogger,
}
