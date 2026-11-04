const { afterEach, beforeEach, describe, expect, it, jest } = require("@jest/globals");
jest.mock("node:child_process", () => ({ execSync: jest.fn() }));
const { execSync } = require("node:child_process");

describe("waitForKafka", () => {
  beforeEach(() => {
    jest.spyOn(console, "log").mockImplementation(() => {});
    execSync.mockReset().mockImplementation((command) => {
      const service = command.match(/custom\.service=(kafka\d)/);
      return service ? `${service[1]}-container\n` : "";
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("uses trimmed container IDs for startup, topic creation, and warmup", () => {
    jest.isolateModules(() => require("./waitForKafka"));

    const commands = execSync.mock.calls.map(([command]) => command);
    for (const service of ["kafka1", "kafka2", "kafka3"]) {
      expect(commands).toEqual(
        expect.arrayContaining([
          expect.stringMatching(new RegExp(`${service}-container[ \\t]+bash -c`)),
        ]),
      );
    }
    expect(commands.filter((command) => command.includes("kafka-topics --create"))).toHaveLength(
      11,
    );
    expect(commands).toEqual(
      expect.arrayContaining([
        expect.stringContaining("docker compose"),
        expect.stringContaining("kafka-consumer-groups"),
      ]),
    );
    expect(execSync).toHaveBeenNthCalledWith(1, expect.stringContaining("docker ps"), {
      encoding: "utf8",
    });
  });

  it("stops startup when a command fails", () => {
    const error = new Error("docker failed");
    execSync.mockImplementationOnce(() => {
      throw error;
    });

    expect(() => jest.isolateModules(() => require("./waitForKafka"))).toThrow(error);
    expect(execSync).toHaveBeenCalledTimes(1);
  });
});
