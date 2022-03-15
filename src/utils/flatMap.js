const flatten = require('./flatten')

module.exports = (arr, mapper) => flatten(arr.map(mapper))
