module.exports = {
  verbose: !!process.env.VERBOSE,
  moduleNameMapper: {
    testHelpers: '<rootDir>/testHelpers/index.js',
  },
  testResultsProcessor: './node_modules/jest-junit',
  testPathIgnorePatterns: ['/node_modules/'],
}
