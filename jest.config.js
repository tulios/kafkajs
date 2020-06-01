module.exports = {
  verbose: !!process.env.VERBOSE,
  moduleNameMapper: {
    testHelpers: '<rootDir>/testHelpers/index.js',
  },
  setupFilesAfterEnv: ['<rootDir>/testHelpers/setup.js'],
  testPathIgnorePatterns: ['/node_modules/'],
  testRunner: 'jest-circus/runner',
  testEnvironment: 'node',
  testRegex: '(/__tests__/.*\\.spec\\.js|(\\.|/)spec)\\.jsx?$',
  reporters: ['default', 'jest-junit'],
  bail: false,
}
