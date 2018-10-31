// From: https://kafka.apache.org/protocol.html#The_Messages_FindCoordinator

/**
 * @typedef {number} CoordinatorType
 *
 * Enum for tri-state values.
 * @enum {CoordinatorType}
 */
module.exports = {
  GROUP: 0,
  TRANSACTION: 1,
}
