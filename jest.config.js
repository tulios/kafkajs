module.exports = {
  verbose: !!process.env.VERBOSE,
  moduleNameMapper: {
    testHelpers: '<rootDir>/testHelpers/index.js',
  },
  setupTestFrameworkScriptFile: '<rootDir>/testHelpers/setup.js',
  testResultsProcessor: './node_modules/jest-junit',
  testPathIgnorePatterns: ['/node_modules/'],
  testEnvironment: 'node',
}
