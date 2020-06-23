const Long = require('long')

module.exports = offset => (!offset && offset !== 0) || Long.fromValue(offset).isNegative()
