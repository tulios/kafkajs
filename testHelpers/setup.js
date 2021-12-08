jest.setTimeout(90000)

const retries = process.env.TEST_RETRIES != null ? parseInt(process.env.TEST_RETRIES, 10) : 0
jest.retryTimes(retries)

require('jest-extended')

expect.extend({
  optional(v, value) {
    const pass = typeof v === 'undefined' || v === value
    return {
      pass,
      message: () => `Expected ${value} to ${pass ? 'not ' : ''}be undefined or ${value}`,
    }
  },
  toBeTypeOrNull(received, argument) {
    if (received === null)
      return {
        message: () => `Ok`,
        pass: true,
      }
    if (expect(received).toEqual(expect.any(argument))) {
      return {
        message: () => `Ok`,
        pass: true,
      }
    } else {
      return {
        message: () => `expected ${received} to be ${argument} type or null`,
        pass: false,
      }
    }
  },
})

const glob = require('glob')
const path = require('path')

// Protocol files are imported on demand depending on the APIs supported by Kafka,
// but this behavior doesn't help Jest. Therefore, all files are eagerly imported
// when running tests
glob
  .sync('src/protocol/requests/**/v*/@(request|response).js')
  .map(file => path.resolve(file))
  .map(require)
