const Long = require('long')
const isNumber = number => /^-?\d+$/.test(number)

module.exports = offset => !isNumber(offset) || Long.fromValue(offset).compare(0) === -1
