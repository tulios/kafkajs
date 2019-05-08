const murmur2 = require('./murmur2')
const createDefaultPartitioner = require('./partitioner')

module.exports = createDefaultPartitioner(murmur2)
