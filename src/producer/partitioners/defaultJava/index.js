const murmur2 = require('./murmur2')
const createDefaultPartitioner = require('../default/partitioner')

module.exports = createDefaultPartitioner(murmur2)
