const requestV0 = require('../v0/request')

module.exports = ({ mechanism }) => ({ ...requestV0(), apiVersion: 1 })
