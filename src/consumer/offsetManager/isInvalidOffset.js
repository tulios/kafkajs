const Long = require('long')

module.exports = offset => !offset || Long.fromValue(offset).compare(0) === -1
