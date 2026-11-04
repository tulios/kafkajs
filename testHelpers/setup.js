const { expect, jest } = require("@jest/globals");

const retries =
  process.env.TEST_RETRIES != null ? Number.parseInt(process.env.TEST_RETRIES, 10) : 0;
jest.retryTimes(retries);

expect.extend(require("jest-extended"));

expect.extend({
  optional(v, value) {
    const pass = typeof v === "undefined" || v === value;
    return {
      pass,
      message: () => `Expected ${value} to ${pass ? "not " : ""}be undefined or ${value}`,
    };
  },
  toBeTypeOrNull(received, argument) {
    if (received === null)
      return {
        message: () => `Ok`,
        pass: true,
      };
    if (expect(received).toEqual(expect.any(argument))) {
      return {
        message: () => `Ok`,
        pass: true,
      };
    } else {
      return {
        message: () => `expected ${received} to be ${argument} type or null`,
        pass: false,
      };
    }
  },
});

const { globSync } = require("node:fs");
const path = require("path");

// Protocol files are imported on demand depending on the APIs supported by Kafka,
// but this behavior doesn't help Jest. Therefore, all files are eagerly imported
// when running tests
globSync("src/protocol/requests/**/v*/@(request|response).js")
  .map((file) => path.resolve(file))
  .map(require);
