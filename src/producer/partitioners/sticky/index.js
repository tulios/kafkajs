const murmur2 = require('../default/murmur2')
const createStickyPartitioner = require('./stickyPartitioner')

module.exports = createStickyPartitioner(murmur2)
