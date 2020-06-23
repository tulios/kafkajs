const Long = require('long')

module.exports = offset => !offset || Long.fromValue(offset).isNegative()
