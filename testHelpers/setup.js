jest.setTimeout(90000)

console.log('=============== JEST SETUP ================')
const retries = process.env.TEST_RETRIES != null ? parseInt(process.env.TEST_RETRIES, 10) : 0
jest.retryTimes(retries)
console.log(`Setting retries to: ${retries}`)
console.log('============= END JEST SETUP ==============')

require('jest-extended')
const glob = require('glob')
const path = require('path')

// Protocol files are imported on demand depending on the APIs supported by Kafka,
// but this behavior doesn't help Jest. Therefore, all files are eagerly imported
// when running tests
glob
  .sync('src/protocol/requests/**/v*/@(request|response).js')
  .map(file => path.resolve(file))
  .map(require)
